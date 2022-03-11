package scheduler

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"math"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/cantor"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/capacity"
	"sort"
	"strings"
)

func PlacePod(ctx context.Context, podEvictor *evictions.PodEvictor, podInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)([]*capacity.PodInfo, *capacity.NodeInfo, bool){
	var migratePods []*capacity.PodInfo
	singleMigratePod, singleMigrateNode, placedSingle := placeCapacity(ctx, podEvictor, podInfo, nodeInfos)
	if placedSingle {
		migratePods = append(migratePods, singleMigratePod)
		return migratePods, singleMigrateNode, placedSingle
	}else {
		multiMigratePods, multiMigrateNode, placedMulti := placeCapacityWithMultipleMigration(ctx, podEvictor, podInfo, nodeInfos)
		if placedMulti {
			migratePods = append(migratePods, multiMigratePods...)
			return migratePods, multiMigrateNode, placedMulti
		}
	}

	return nil, nil, false
}

func placeCapacity(ctx context.Context, podEvictor *evictions.PodEvictor, placePodInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)(*capacity.PodInfo, *capacity.NodeInfo, bool) {
	if checkPlacementElibility(placePodInfo, nodeInfos) {
		fromNode, toNode, directlyPlaceOnNode := computeNormalPlacement(placePodInfo, nodeInfos)
		if directlyPlaceOnNode {
			klog.V(1).Infof("migrate pod namespace:%s name:%s from node:%s to node:%s", placePodInfo.Pod.GetNamespace(), placePodInfo.Pod.GetName(), fromNode.Node().GetName(), toNode.Node().GetName())
			if err := MigratePod(ctx, podEvictor, placePodInfo.Pod, fromNode.Node(), toNode.Node(), false); err != nil {
				klog.V(1).ErrorS(err, "migrate pod", placePodInfo.Pod.GetNamespace(), placePodInfo.Pod.GetName())
				return nil, nil, false
			}

			if fromNode != nil {
				fromNode.RemovePod(placePodInfo.Pod)
				return placePodInfo, fromNode, true
			}
			return nil, nil, false
		}else {
			sortedNodeNames := computePlacementPriority(placePodInfo, nodeInfos)
			for _, name := range sortedNodeNames {
				var nodeInfo *capacity.NodeInfo
				for _, nodeInfo = range nodeInfos {
					if nodeInfo.Node().GetName() == name {
						break
					}
				}
				eligiblePods := computeEligiblePods(placePodInfo, nodeInfo)
				if len(eligiblePods) != 0 {
					eligiblePod := computeMinimumMigrateablePod(eligiblePods)
					return placeCapacity(ctx, podEvictor, eligiblePod, nodeInfos)
				}
			}
		}
	}

	return nil, nil, false
}

func checkPlacementElibility(podInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)bool{
	var cpuSum, memorySum int64
	sr := make(map[v1.ResourceName]int64)
	var nodes []*v1.Node
	for _, nodeInfo := range nodeInfos {
		cpuSum += nodeInfo.Available.MilliCPU
		memorySum += nodeInfo.Allocatable.Memory
		for rName, rQuant := range nodeInfo.Allocatable.ScalarResources {
			sr[rName] += rQuant
		}
		nodes = append(nodes, nodeInfo.Node())
	}
	res, non0CPU, non0Mem := capacity.CalculateResource(podInfo.Pod)
	// ER资源使否满足
	isUnAvailable := false
	for rName, rQuant := range res.ScalarResources {
		if sr[rName] < rQuant {
			isUnAvailable = true
			break
		}
	}
	if cpuSum >= non0CPU && memorySum >= non0Mem && !isUnAvailable && nodeutil.PodFitsAnyOtherNode(podInfo.Pod, nodes){
		return true
	}

	return false
}

func computeNormalPlacement(placePodInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)(*capacity.NodeInfo, *capacity.NodeInfo, bool){
	var fromNode, toNode *capacity.NodeInfo

	for _, nodeInfo := range nodeInfos {
		if nodeInfo.Node().GetName() == placePodInfo.Pod.Spec.NodeName {
			fromNode = nodeInfo
		}
	}
	for _, nodeInfo := range nodeInfos {
		if placePodInfo.Pod.Spec.NodeName == nodeInfo.Node().GetName() || !nodeutil.PodFitsCurrentNode(placePodInfo.Pod, nodeInfo.Node()) {
			continue
		}
		res, placeCpu, placeMem := capacity.CalculateResource(placePodInfo.Pod)
		// ER资源使否不满足
		isUnAvailable := false
		for rName, rQuant := range res.ScalarResources {
			if nodeInfo.Available.ScalarResources[rName] < rQuant {
				isUnAvailable = true
				break
			}
		}
		if placeCpu <= nodeInfo.Available.MilliCPU && placeMem <= nodeInfo.Available.Memory && !isUnAvailable {
			toNode = nodeInfo
			return fromNode, toNode, true
		}
	}

	return fromNode, toNode, false
}

func computePlacementPriority(podInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)[]string{
	nodeScore := make(map[string]int)
	keys := make([]string, 0, len(nodeInfos))
	for _, nodeInfo := range nodeInfos {
		res, cpu, mem := capacity.CalculateResource(podInfo.Pod)
		var pairedScore int
		keys = append(keys, nodeInfo.Node().GetName())
		for rName, rQuant := range res.ScalarResources {
			diffSr := rQuant - nodeInfo.Available.ScalarResources[rName]
			pairedScore += cantor.Pair(int(math.Abs(float64(diffSr))), int(math.Abs(float64(cpu - nodeInfo.Available.MilliCPU))))
			pairedScore += cantor.Pair(int(math.Abs(float64(diffSr))), int(math.Abs(float64(mem - nodeInfo.Available.Memory)/capacity.MB)))
		}
		pairedScore += cantor.Pair(int(math.Abs(float64(cpu - nodeInfo.Available.MilliCPU))), int(math.Abs(float64(mem - nodeInfo.Available.Memory)/capacity.MB)))
		//pairedScore = cantor.Pair(int(math.Abs(float64(cpu - nodeInfo.Available.MilliCPU))), int(math.Abs(float64(mem - nodeInfo.Available.Memory)/capacity.MB)))
		nodeScore[nodeInfo.Node().GetName()] = pairedScore
	}

	sort.Slice(keys, func(i, j int) bool { return nodeScore[keys[i]] < nodeScore[keys[j]] })

	return keys
}

func computeEligiblePods(placePodInfo *capacity.PodInfo, nodeInfo *capacity.NodeInfo)[]*capacity.PodInfo {
	if !nodeutil.PodFitsCurrentNode(placePodInfo.Pod, nodeInfo.Node()) {
		return nil
	}

	var podInfos []*capacity.PodInfo
	placeRes, placeCpu, placeMem := capacity.CalculateResource(placePodInfo.Pod)
	for _, podInfo := range nodeInfo.Pods {
		if !capacity.IsMigratable(podInfo.Pod) {
			continue
		}
		migrateRes, migrateCpu, migrateMem := capacity.CalculateResource(podInfo.Pod)
		isUnAvailable := false
		for rName, rQuant := range placeRes.ScalarResources {
			if _, ok := nodeInfo.Available.ScalarResources[rName]; ok{
				diffSr := rQuant - nodeInfo.Available.ScalarResources[rName]
				if migrateRes.ScalarResources[rName] < diffSr || migrateRes.ScalarResources[rName] >= rQuant{
					isUnAvailable = true
					break
				}
			}
		}
		diffCpu, diffMem := placeCpu - nodeInfo.Available.MilliCPU, placeMem - nodeInfo.Available.Memory
		if migrateCpu >= diffCpu && migrateMem >= diffMem && migrateCpu < placeCpu && migrateMem < placeMem && !isUnAvailable {
			podInfos = append(podInfos, podInfo)
		}
	}

	return podInfos
}

func computeMinimumMigrateablePod(eligiblePods []*capacity.PodInfo)*capacity.PodInfo {
	if len (eligiblePods) == 0 {
		return nil
	}

	if len(eligiblePods) == 1 {
		return eligiblePods[0]
	}
	
	sort.Slice(eligiblePods, func(i, j int) bool {
		res1, cpu1, _ := capacity.CalculateResource(eligiblePods[i].Pod)
		res2, cpu2, _ := capacity.CalculateResource(eligiblePods[j].Pod)
		for rName, rQuant1 := range res1.ScalarResources {
			if rQuant2, ok := res2.ScalarResources[rName]; ok {
				if rQuant1 != rQuant2 {
					return rQuant1 < rQuant2
				}
			}
		}

		return cpu1 < cpu2
	})

	return eligiblePods[0]
}

func placeCapacityWithMultipleMigration(ctx context.Context, podEvictor *evictions.PodEvictor, placePodInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)([]*capacity.PodInfo, *capacity.NodeInfo, bool){
	if !checkPlacementElibility(placePodInfo, nodeInfos) {
		fromNode, toNode, directlyPlaceOnNode := computeNormalPlacement(placePodInfo, nodeInfos)
		if directlyPlaceOnNode {
			if err := MigratePod(ctx, podEvictor, placePodInfo.Pod, fromNode.Node(), toNode.Node(), false); err != nil {
				klog.V(1).ErrorS(err, "migrate pod", placePodInfo.Pod.GetNamespace(), placePodInfo.Pod.GetName())
				return nil, nil, false
			}
			for _, nodeInfo := range nodeInfos {
				if nodeInfo.Node().GetName() == placePodInfo.Pod.Spec.NodeName {
					return nil, nodeInfo, true
				}
			}
			return nil, nil, false
		}else {
			sortedNodeNames := computePlacementPriority(placePodInfo, nodeInfos)
			for _, name := range sortedNodeNames {
				var nodeInfo *capacity.NodeInfo
				for _, nodeInfo = range nodeInfos {
					if nodeInfo.Node().GetName() == name {
						break
					}
				}
				eligiblePods := computeMultipleEligiblePods(placePodInfo, nodeInfo)
				if len(eligiblePods) != 0 {
					currentEligiblePods := []*capacity.PodInfo{eligiblePods[0]}
					pods := computeMinimumMigrateablePods(placePodInfo, eligiblePods, currentEligiblePods, nodeInfo)
					for _, pod := range pods {
						if _, _, ok := placeCapacity(ctx, podEvictor, pod, nodeInfos); !ok {
							placeCapacityWithMultipleMigration(ctx, podEvictor, pod, nodeInfos)
						}
					}
					return pods, nodeInfo, true
				}
			}
		}
	}
	return nil, nil, false
}

func computeMultipleEligiblePods(placePodInfo *capacity.PodInfo, nodeInfo *capacity.NodeInfo)[]*capacity.PodInfo{
	var podInfos []*capacity.PodInfo
	placeRes, placeCpu, placeMem := capacity.CalculateResource(placePodInfo.Pod)
	for _, podInfo := range nodeInfo.Pods {
		migragteRes, migrateCpu, migrateMem := capacity.CalculateResource(podInfo.Pod)
		isUnSatisfied := false
		for rName, rPlaceQuant := range placeRes.ScalarResources {
			if rMigrateQuant, ok := migragteRes.ScalarResources[rName]; ok {
				if rMigrateQuant >= rPlaceQuant {
					isUnSatisfied = true
					break
				}
			}
		}
		if migrateCpu < placeCpu && migrateMem < placeMem && !isUnSatisfied{
			podInfos = append(podInfos, podInfo)
		}
	}

	return podInfos
}

func computeMinimumMigrateablePods(placePodInfo *capacity.PodInfo, multipleEligiblePods, currentEligiblePods []*capacity.PodInfo, nodeInfo *capacity.NodeInfo)[]*capacity.PodInfo{
	totalCpuMiliCore := computeTotalCpuMilicore(currentEligiblePods...)
	totalMemMB := computeTotalMemoryMB(currentEligiblePods...)
	placedRes, placeCpu, placeMem := capacity.CalculateResource(placePodInfo.Pod)
	isUnAvailable := false
	for rName, rQuant := range placedRes.ScalarResources {
		totalSr := computeTotalScalarResource(rName, currentEligiblePods...)
		if totalSr < rQuant {
			isUnAvailable = true
			break
		}
	}

	if !isUnAvailable {
		cpuDiff, memDiff := placeCpu - nodeInfo.Available.MilliCPU, placeMem - nodeInfo.Available.Memory
		if cpuDiff > 0 && memDiff > 0 {
			if totalCpuMiliCore >= cpuDiff && totalMemMB >= memDiff {
				return sortMigrateablePods(currentEligiblePods)
			}
		} else if cpuDiff > 0 && memDiff < 0 {
			if totalCpuMiliCore >= cpuDiff {
				return sortMigrateablePods(currentEligiblePods)
			}
		} else if memDiff > 0 && cpuDiff < 0 {
			if totalCpuMiliCore >= cpuDiff {
				return sortMigrateablePods(currentEligiblePods)
			}
		}

		if len(multipleEligiblePods) <= 1 {
			return sortMigrateablePods(currentEligiblePods)
		}
	}
	currentPod := multipleEligiblePods[1]
	currentEligiblePods = append(currentEligiblePods, currentPod)
	return computeMinimumMigrateablePods(placePodInfo, multipleEligiblePods[1:], currentEligiblePods, nodeInfo)
}

func computeTotalMemoryMB(podInfos ...*capacity.PodInfo)int64 {
	memSum := int64(0)
	for _, podInfo := range podInfos {
		_, _, mem := capacity.CalculateResource(podInfo.Pod)
		memSum += mem
	}
	return int64(float64(memSum)/capacity.MB)
}

func computeTotalCpuMilicore(podInfos ...*capacity.PodInfo)int64{
	cpuSum := int64(0)
	for _, podInfo := range podInfos {
		_, _, cpu := capacity.CalculateResource(podInfo.Pod)
		cpuSum += cpu
	}
	return cpuSum
}

func computeTotalScalarResource(name v1.ResourceName, podInfos ...*capacity.PodInfo)int64{
	sum := int64(0)
	for _, podInfo := range podInfos {
		res, _, _ := capacity.CalculateResource(podInfo.Pod)
		if rQuaint, ok := res.ScalarResources[name]; ok {
			sum += rQuaint
		}
	}
	return sum
}

func sortMigrateablePods(pods []*capacity.PodInfo)[]*capacity.PodInfo{
	podScore := make(map[string]int)
	keys := make([]string, 0, len(pods))
	for _, pod := range pods {
		key := pod.Pod.GetNamespace() + "-" + pod.Pod.GetName()
		keys = append(keys, key)
		var pairedScore int
		res, cpu, mem := capacity.CalculateResource(pod.Pod)
		for _, rQuant := range res.ScalarResources {
			pairedScore += cantor.Pair(int(math.Abs(float64(rQuant))), int(math.Abs(float64(cpu))))
			pairedScore += cantor.Pair(int(math.Abs(float64(rQuant))), int(math.Abs(float64(mem)/capacity.MB)))
		}
		pairedScore += cantor.Pair(int(math.Abs(float64(cpu))), int(math.Abs(float64(mem)/capacity.MB)))
		podScore[key] = pairedScore
	}

	sort.Slice(keys, func(i, j int) bool { return podScore[keys[i]] < podScore[keys[j]] })

	var sortPods []*capacity.PodInfo
	for _, key := range keys {
		strs := strings.Split(key, "-")
		if len(strs) < 2 {
			continue
		}
		ns := strs[0]
		name := strs[1]
		for _ , pod := range pods {
			if pod.Pod.GetNamespace() == ns && pod.Pod.GetName() == name {
				sortPods = append(sortPods, pod)
			}
		}
	}

	return sortPods
}
