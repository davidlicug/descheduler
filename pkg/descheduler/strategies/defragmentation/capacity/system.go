package capacity

import (
	"context"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
)

func GetSystemSnapshot(ctx context.Context,	client clientset.Interface, nodes []*v1.Node)[]*NodeInfo {
	var nodeInfos []*NodeInfo
	for _, node := range nodes {
		pods, err := podutil.ListPodsOnANode(ctx, client, node)
		if err != nil {
			klog.V(2).InfoS("Node will not be processed, error accessing its pods", "node", klog.KObj(node), "err", err)
			continue
		}
		nodeInfo := NewNodeInfo()
		nodeInfo.SetNode(node)
		for _, pod := range pods {
			nodeInfo.AddPod(pod)
		}
		nodeInfos = append(nodeInfos, nodeInfo)
	}

	return nodeInfos
}

func GetNodeResourceUsage(nodeInfos ...*NodeInfo)(int64, int64, map[string]int64, int64, int64, map[string]int64, int64, int64, map[string]int64){
	var totalCpu, totalMem int64
	var usedCpu, usedMem int64
	totalSr := make(map[string]int64)
	usedSr := make(map[string]int64)
	availableSr := make(map[string]int64)
	var availableCpu, availableMem int64
	for _, nodeInfo := range nodeInfos {
		totalCpu += nodeInfo.Allocatable.MilliCPU
		totalMem += nodeInfo.Allocatable.Memory
		for rName, rQuanta := range nodeInfo.Allocatable.ScalarResources {
			totalSr[string(rName)] += rQuanta
		}
		usedCpu += nodeInfo.NonZeroRequested.MilliCPU
		usedMem += nodeInfo.NonZeroRequested.Memory
		for rName, rQuanta := range nodeInfo.NonZeroRequested.ScalarResources {
			usedSr[string(rName)] += rQuanta
		}
		availableCpu += nodeInfo.Available.MilliCPU
		availableMem += nodeInfo.Available.Memory
		for rName, rQuanta := range nodeInfo.Available.ScalarResources {
			availableSr[string(rName)] += rQuanta
		}
	}

	return totalCpu, totalMem, totalSr, usedCpu, usedMem, usedSr, availableCpu, availableMem, availableSr
}

