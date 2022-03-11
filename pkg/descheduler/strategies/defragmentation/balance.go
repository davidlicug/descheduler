package defragmentation

import (
	"context"
	"fmt"
	"github.com/mohae/deepcopy"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/capacity"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/scheduler"
	"time"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

const (
	balanceIterations = 10
)

func BalancePodsOnNodeForDefragmentation(
	ctx context.Context,
	client clientset.Interface,
	strategy api.DeschedulerStrategy,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
){
	err := validateAndParseBalancePodsParams(strategy.Params)
	if err != nil {
		klog.ErrorS(err, "Invalid BalancePodsOnNodeForDefragmentation parameters")
		return
	}
	klog.V(1).Infoln("***********************************************************************************")
	klog.V(1).Infoln("Trying to balance the cpu/memory consumption across nodes")
	klog.V(1).Infoln("***********************************************************************************")

	iterations := *strategy.Params.Iterations
	if iterations == 0 {
		iterations = balanceIterations
	}

	if err := BalancePolicy(ctx, client, nodes, podEvictor, iterations); err != nil {
		klog.V(1).ErrorS(err, "balance the cpu/memory consumption across nodes")
		return
	}

	return
}

// BalancePolicy actual scheduler
func BalancePolicy(ctx context.Context,
			 client clientset.Interface,
			 nodes []*v1.Node,
			 podEvictor *evictions.PodEvictor,
			 iterations int32,
)error{
	nodeInfos := capacity.GetSystemSnapshot(ctx, client, nodes)
	pivotRatio := capacity.GetPivotRatio(nodeInfos)
	entropyBeforeBalancing := capacity.GetSystemEntropy(nodeInfos)

	nodeInfos = capacity.GetSystemSnapshot(ctx, client, nodes)
	totalCpu, totalMem, totalSr, usedCpu, usedMem, usedSr, availableCpu, availableMem, availableSr := capacity.GetNodeResourceUsage(nodeInfos...)
	klog.V(1).Infof("Nodes resource usage: total-cpu:%.3f, total-memory:%.3fMi, used-cpu:%.3f, used-memory:%.3fMi, available-cpu:%.3f, available-memory:%.3fMi", float64(totalCpu)/1000, float64(totalMem)/capacity.MB, float64(usedCpu)/1000, float64(usedMem)/capacity.MB, float64(availableCpu)/1000, float64(availableMem)/capacity.MB)
	for rName, rQuanta := range totalSr {
		klog.V(1).Infof("Nodes er usage: %s: total:%d, used:%d, available:%d", rName, rQuanta, usedSr[rName], availableSr[rName])
	}
	klog.V(1).Infof("Nodes usage: used-cpu(%%):%.2f%%, used-memory(%%):%.2f%%, fragment-cpu(%%):%.2f%%, fragment-memory(%%):%.2f%%", 100*float64(usedCpu)/float64(totalCpu), 100*float64(usedMem)/float64(totalMem), 100*float64(availableCpu)/float64(totalCpu), 100*float64(availableMem)/float64(totalMem))
	for rName, rQuanta := range availableSr {
		klog.V(1).Infof("Nodes er usage: %s used(%%):%.2f%%, fragment(%%):%.2f%%", rName, 100*float64(usedSr[rName])/float64(totalSr[rName]), 100*float64(rQuanta)/float64(totalSr[rName]))
	}
	klog.V(1).Infoln(" Nodes information before balancing :")
	for _, nodeInfo := range nodeInfos {
		klog.V(1).InfoS("-", "node", nodeInfo.Node().GetName())
		for _, podInfo := range nodeInfo.Pods {
			klog.V(1).InfoS(" |-", "pod", klog.KObj(podInfo.Pod))
		}
	}

	var currIterations int32
	for currIterations < iterations && isSchedulingDone(nodeInfos, pivotRatio) {
		klog.V(1).Infof("This is the %d iteration" , currIterations+1)
		klog.V(1).Infoln("***********************************************************************************")

		if err := BalanceWorkload(ctx, client, nodes, podEvictor); err != nil {
			klog.V(1).ErrorS(err, "Failed to balance work load")
			return err
		}

		time.Sleep(30)
		klog.V(1).Infoln("***********************************************************************************")
		nodeInfos = capacity.GetSystemSnapshot(ctx, client, nodes)
		totalCpu, totalMem, totalSr, usedCpu, usedMem, usedSr, availableCpu, availableMem, availableSr = capacity.GetNodeResourceUsage(nodeInfos...)
		klog.V(1).Infof("Nodes resource usage: total-cpu:%.3f, total-memory:%.3fMi, used-cpu:%.3f, used-memory:%.3fMi, available-cpu:%.3f, available-memory:%.3fMi", float64(totalCpu)/1000, float64(totalMem)/capacity.MB, float64(usedCpu)/1000, float64(usedMem)/capacity.MB, float64(availableCpu)/1000, float64(availableMem)/capacity.MB)
		for rName, rQuanta := range totalSr {
			klog.V(1).Infof("Nodes er usage: %s: total:%d, used:%d, available:%d", rName, rQuanta, usedSr[rName], availableSr[rName])
		}
		klog.V(1).Infof("Nodes usage: used-cpu(%%):%.2f%%, used-memory(%%):%.2f%%, fragment-cpu(%%):%.2f%%, fragment-memory(%%):%.2f%%", 100*float64(usedCpu)/float64(totalCpu), 100*float64(usedMem)/float64(totalMem), 100*float64(availableCpu)/float64(totalCpu), 100*float64(availableMem)/float64(totalMem))
		for rName, rQuanta := range availableSr {
			klog.V(1).Infof("Nodes er usage: %s used(%%):%.2f%%, fragment(%%):%.2f%%", rName, 100*float64(usedSr[rName])/float64(totalSr[rName]), 100*float64(rQuanta)/float64(totalSr[rName]))
		}
		entropyAfterBalancing := capacity.GetSystemEntropy(nodeInfos)
		klog.V(1).Infof("System entropy before  balancing : %f", entropyBeforeBalancing)
		klog.V(1).Infof("System entropy after  balancing : %f", entropyAfterBalancing)
		klog.V(1).Infoln(" Nodes information after balancing :")
		for _, nodeInfo := range nodeInfos {
			klog.V(1).InfoS("-", "node", nodeInfo.Node().GetName())
			for _, podInfo := range nodeInfo.Pods {
				klog.V(1).InfoS(" |-", "pod", klog.KObj(podInfo.Pod))
			}
		}

		currIterations++
	}

	return nil
}

func BalanceWorkload(ctx context.Context,
	client clientset.Interface,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
)error{
	var nodeA, nodeB *v1.Node
	var podA, podB *v1.Pod
	var balanceNodeInfos []*capacity.NodeInfo
	nodeInfos := capacity.GetSystemSnapshot(ctx, client, nodes)
	pivotRatio := capacity.GetPivotRatio(nodeInfos)
	entropyBeforeBalancing := capacity.GetSystemEntropy(nodeInfos)
	capacity.SortNodesBasedRatio(nodeInfos)
	leftNodeIndex, rightNodeIndex := 0, len(nodeInfos) - 1

	toBalance := false
	klog.V(1).Infof("nodeInfos %d pivotRatio:%f %d pivotRatio:%f, pivotRatio:%f", leftNodeIndex, capacity.GetCpuMemoryRatio(nodeInfos[leftNodeIndex]), rightNodeIndex, capacity.GetCpuMemoryRatio(nodeInfos[rightNodeIndex]), pivotRatio)
	for capacity.GetCpuMemoryRatio(nodeInfos[leftNodeIndex]) < pivotRatio && capacity.GetCpuMemoryRatio(nodeInfos[rightNodeIndex]) > pivotRatio {
		//Recompute the distance for both the nodes because after swap available capacity might have changed
		leftNodeDistance := capacity.GetDistanceFromPivot(nodeInfos[leftNodeIndex], pivotRatio)
		rightNodeDistance := capacity.GetDistanceFromPivot(nodeInfos[rightNodeIndex], pivotRatio)

		//Left side of pivot has cpu intensive pods so node has more memory available
		// We will pick pod with max cpu for balancing
		cpuPodInfos := capacity.SortPodsBasedOnCpu(nodeInfos[leftNodeIndex].Pods)
		//Right side of pivot has memory intensive pods so node has more cpu available
		// We will pick pod with max memory for balancing
		memPodInfos := capacity.SortPodsBasedOnMemory(nodeInfos[rightNodeIndex].Pods)

		leftPodIndex, rightPodIndex := 0, 0
		isSwapped := false
		for leftPodIndex < len(cpuPodInfos) && rightPodIndex < len(memPodInfos) {
			//if !capacity.IsMigrated(cpuPodInfos[leftPodIndex].Pod) || !nodeutil.PodFitsCurrentNode(cpuPodInfos[leftPodIndex].Pod, nodeInfos[rightNodeIndex].Node()){
			if !capacity.IsMigratable(cpuPodInfos[leftPodIndex].Pod){
				leftPodIndex++
				continue
			}
			//if !capacity.IsMigrated(memPodInfos[rightPodIndex].Pod) || !nodeutil.PodFitsCurrentNode(memPodInfos[rightNodeIndex].Pod, nodeInfos[leftNodeIndex].Node()){
			if !capacity.IsMigratable(memPodInfos[rightPodIndex].Pod) {
				rightPodIndex++
				continue
			}

			balanceNodeInfos = deepcopy.Copy(nodeInfos).([]*capacity.NodeInfo)
			isSwapped = swapIfPossible(balanceNodeInfos, balanceNodeInfos[leftNodeIndex], balanceNodeInfos[rightNodeIndex], cpuPodInfos[leftPodIndex], memPodInfos[rightPodIndex])
			if isSwapped {
				break
			}

			if leftNodeDistance < rightNodeDistance {
				if leftPodIndex < len(nodeInfos[leftNodeIndex].Pods) -1 {
					leftPodIndex++
				}else {
					leftPodIndex = 0
					rightPodIndex++
				}
			}else {
				if rightPodIndex < len(nodeInfos[rightNodeIndex].Pods) - 1 {
					rightPodIndex++
				}else{
					rightPodIndex = 0
					leftPodIndex++
				}
			}
		}

		if isSwapped {
			toBalance = true
			nodeA, nodeB = nodeInfos[leftNodeIndex].Node(), nodeInfos[rightNodeIndex].Node()
			podA, podB = cpuPodInfos[leftPodIndex].Pod, memPodInfos[rightPodIndex].Pod
			break
		}

		if leftNodeDistance < rightNodeDistance {
			if capacity.GetCpuMemoryRatio(nodeInfos[leftNodeIndex + 1]) < pivotRatio {
				leftNodeIndex++
			}else {
				leftNodeIndex = 0
				rightNodeIndex--
			}
		}else{
			if capacity.GetCpuMemoryRatio(nodeInfos[rightNodeIndex - 1]) > pivotRatio {
				rightNodeIndex--
			}else{
				rightNodeIndex = len(nodeInfos) - 1
				leftNodeIndex++
			}
		}
	}

	if toBalance {
		err := scheduler.SwapPods(ctx, podEvictor, podA, nodeA, podB, nodeB)
		if err != nil {
			klog.V(1).ErrorS(err, "Swapping pod", "pod", klog.KObj(podA), "running on node", "node", klog.KObj(nodeA), "with pod", "pod", klog.KObj(podB), "running on node", "node", klog.KObj(nodeB))
			return err
		}
		time.Sleep(30)
		nodeInfos = capacity.GetSystemSnapshot(ctx, client, nodes)
		pivotRatio = capacity.GetPivotRatio(nodeInfos)
		capacity.SortNodesBasedRatio(nodeInfos)
		klog.V(1).InfoS("swapping pod", "node", klog.KObj(nodeA), "pod", klog.KObj(podA), "node", klog.KObj(nodeB), "pod", klog.KObj(podB))
		//klog.V(1).InfoS("swap is successful for node", "node", klog.KObj(nodeA), "pod", klog.KObj(podB), "node", klog.KObj(nodeB), "pod", klog.KObj(podA))
		entropyAfterBalancing := capacity.GetSystemEntropy(balanceNodeInfos)
		klog.V(1).Infof("Swap is successful and entropy changed from %f to %f", entropyBeforeBalancing, entropyAfterBalancing)
		entropyBeforeBalancing = entropyAfterBalancing
	}

	return nil
}

// check if in the system all are on each side of pivot ratio
// all the nodes are on one side of the pivot ratio. No mode balancing of CPU/Memory possible
func isSchedulingDone(nodeInfos []*capacity.NodeInfo, pivotRatio float64)bool{
	var positiveCount, negativeCount int
	for _, nodeInfo := range nodeInfos {
		v := pivotRatio - capacity.GetCpuMemoryRatio(nodeInfo)
		if v > 0 {
			positiveCount++
		}else if v < 0 {
			negativeCount++
		}
	}

	if positiveCount == len(nodeInfos) || negativeCount == len(nodeInfos) {
		return true
	}

	return false
}

func swapIfPossible(nodeInfos []*capacity.NodeInfo, nodeA, nodeB *capacity.NodeInfo, podA, podB *capacity.PodInfo)bool{
	resA, _, _ := capacity.CalculateResource(podA.Pod)
	resB, _, _ := capacity.CalculateResource(podB.Pod)
	// ER资源使否不满足
	isSatisfied := true
	for rName, rQuantaA := range resA.ScalarResources {
		rQuantaB, ok := resB.ScalarResources[rName]
		if !ok {
			isSatisfied = false
			break
		}
		if rQuantaB != rQuantaA {
			isSatisfied = false
			break
		}
	}

	for rName, rQuantaB := range resB.ScalarResources {
		rQuantaA, ok := resA.ScalarResources[rName]
		if !ok {
			isSatisfied = false
			break
		}
		if rQuantaB != rQuantaA {
			isSatisfied = false
			break
		}
	}

	if !isSatisfied {
		return false
	}

	entropyBeforeSwap := capacity.GetSystemEntropy(nodeInfos)

	aRemovedFromA := nodeA.RemovePod(podA.Pod)
	bRemovedFromB := nodeB.RemovePod(podB.Pod)

	var entropyAfterSwap float64
	if aRemovedFromA == nil && bRemovedFromB == nil {
		nodeA.AddPod(podB.Pod)
		nodeB.AddPod(podA.Pod)

		entropyAfterSwap = capacity.GetSystemEntropy(nodeInfos)
		if entropyAfterSwap >= entropyBeforeSwap {
			//Revert swapping
			if nodeA.RemovePod(podB.Pod) != nil && nodeB.RemovePod(podA.Pod) != nil {
				nodeA.AddPod(podA.Pod)
				nodeB.AddPod(podB.Pod)
			}
			return false
		}

	}else if aRemovedFromA == nil {
		nodeA.AddPod(podA.Pod)
		return false
	}else if bRemovedFromB == nil{
		nodeB.AddPod(podB.Pod)
		return false
	}

	return true
}

func validateAndParseBalancePodsParams(params *api.StrategyParameters) error {
	if params == nil {
		return fmt.Errorf("BalancePodsParams is empty")
	}

	if *params.Iterations < 0 {
		return fmt.Errorf("BalancePodsParams is empty")
	}

	return nil
}