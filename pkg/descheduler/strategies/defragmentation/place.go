package defragmentation

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/capacity"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/scheduler"
	"sigs.k8s.io/descheduler/pkg/utils"
	"sort"
	"time"

	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

const (
	migrateIterations = 10
)

//PlacePodsOnNodeForDefragmentation place the pod on the node
func PlacePodsOnNodeForDefragmentation(
	ctx context.Context,
	client clientset.Interface,
	strategy api.DeschedulerStrategy,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
){
	err := validateAndParsePlacePodsParams(strategy.Params)
	if err != nil {
		klog.V(1).ErrorS(err, "Invalid PlacePodsOnNodeForDefragmentation parameters")
		return
	}

	klog.V(1).Infoln("***********************************************************************************")
	klog.V(1).Infoln("Trying to place pod across nodes")
	klog.V(1).Infoln("***********************************************************************************")

	iterations := *strategy.Params.Iterations
	if iterations == 0 {
		iterations = migrateIterations
	}

	if err := PlacePolicy(ctx, client, strategy, nodes, podEvictor, iterations); err != nil {
		klog.V(1).ErrorS(err, "place pod across nodes")
	}

	return
}

func PlacePolicy(ctx context.Context,
	client clientset.Interface,
	strategy api.DeschedulerStrategy,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
	iterations int32,
)error{
	var includedNamespaces, excludedNamespaces []string
	if strategy.Params != nil && strategy.Params.Namespaces != nil {
		includedNamespaces = strategy.Params.Namespaces.Include
		excludedNamespaces = strategy.Params.Namespaces.Exclude
	}

	var labelSelector *metav1.LabelSelector
	if strategy.Params != nil {
		labelSelector = strategy.Params.LabelSelector
	}

	nodeInfos := capacity.GetSystemSnapshot(ctx, client, nodes)
	var currIterations int32
	for currIterations <  iterations {
		pods, err := podutil.ListPods(ctx,
			client,
			podutil.WithFilter(func(pod *v1.Pod) bool {
				return utils.IsPendingPod(pod)
			}),
			podutil.WithFilter(capacity.IsPlaceable),
			podutil.WithNamespaces(includedNamespaces),
			podutil.WithoutNamespaces(excludedNamespaces),
			podutil.WithLabelSelector(labelSelector),
		)
		if err != nil {
			klog.V(1).ErrorS(err, "list pod error")
			return err
		}

		if len(pods) == 0 {
			klog.V(1).InfoS("No pending pod", "includedNamespaces", includedNamespaces, "excludedNamespaces", excludedNamespaces, "labelSelector", labelSelector)
			break
		}

		sort.Slice(pods, func(i, j int) bool {
			if *pods[i].Spec.Priority != *pods[j].Spec.Priority {
				return *pods[i].Spec.Priority < *pods[j].Spec.Priority
			}

			t1 := pods[i].GetCreationTimestamp()
			t2 := pods[j].GetCreationTimestamp()
			return t1.Before(&t2)
		})

		klog.V(1).Infoln("***********************************************************************************")
		totalCpu, totalMem, totalSr, usedCpu, usedMem, usedSr, availableCpu, availableMem, availableSr := capacity.GetNodeResourceUsage(nodeInfos...)
		klog.V(1).Infof("Nodes resource usage: total-cpu:%.3f, total-memory:%.3fMi, used-cpu:%.3f, used-memory:%.3fMi, available-cpu:%.3f, available-memory:%.3fMi", float64(totalCpu)/1000, float64(totalMem)/capacity.MB, float64(usedCpu)/1000, float64(usedMem)/capacity.MB, float64(availableCpu)/1000, float64(availableMem)/capacity.MB)
		for rName, rQuanta := range totalSr {
			klog.V(1).Infof("Nodes er usage: %s: total:%d, used:%d, available:%d", rName, rQuanta, usedSr[rName], availableSr[rName])
		}
		klog.V(1).Infof("Nodes usage: used-cpu(%%):%.2f%%, used-memory(%%):%.2f%%, fragment-cpu(%%):%.2f%%, fragment-memory(%%):%.2f%%", 100*float64(usedCpu)/float64(totalCpu), 100*float64(usedMem)/float64(totalMem), 100*float64(availableCpu)/float64(totalCpu), 100*float64(availableMem)/float64(totalMem))
		for rName, rQuanta := range availableSr {
			klog.V(1).Infof("Nodes er usage: %s used(%%):%.2f%%, fragment(%%):%.2f%%", rName, 100*float64(usedSr[rName])/float64(totalSr[rName]), 100*float64(rQuanta)/float64(totalSr[rName]))
		}
		klog.V(1).Infoln(" Nodes information before migrating :")
		for _, nodeInfo := range nodeInfos {
			klog.V(1).InfoS("-", "node", klog.KObj(nodeInfo.Node()))
			for _, podInfo := range nodeInfo.Pods {
				klog.V(1).InfoS(" |-", "pod", klog.KObj(podInfo.Pod))
			}
		}

		pod := pods[0]
		podInfo := capacity.NewPodInfo(pod)
		if err := PlaceWorkload(ctx, podEvictor, podInfo, nodeInfos); err != nil {
			klog.V(1).ErrorS(err, "place", podInfo.Pod.GetNamespace(), podInfo.Pod.GetName())
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
		klog.V(1).Infoln(" Nodes information after migrating :")
		for _, nodeInfo := range nodeInfos {
			klog.V(1).InfoS("-", "node", klog.KObj(nodeInfo.Node()))
			for _, podInfo := range nodeInfo.Pods {
				klog.V(1).InfoS(" |-", "pod", klog.KObj(podInfo.Pod))
			}
		}

		currIterations++
	}

	return nil
}

func PlaceWorkload(ctx context.Context, podEvictor *evictions.PodEvictor, podInfo *capacity.PodInfo, nodeInfos []*capacity.NodeInfo)error{
	//bSinglePod := false
	//if len(podInfo.Pod.GetOwnerReferences()) == 0 {
	//	bSinglePod = true
	//}
	//
	//if bSinglePod {
	//	if err := podEvictor.Client().CoreV1().Pods(podInfo.Pod.GetNamespace()).Delete(ctx, podInfo.Pod.GetName(), metav1.DeleteOptions{}); err != nil {
	//		klog.V(1).ErrorS(err, "delete pod", "pod", klog.KObj(podInfo.Pod))
	//		return err
	//	}
	//}

	klog.V(1).InfoS("start to place pending pod", "pod", klog.KObj(podInfo.Pod))
	_, toNodeInfo, ok := scheduler.PlacePod(ctx, podEvictor, podInfo, nodeInfos)
	if !ok {
		//if bSinglePod {
		//	if _, err := podEvictor.Client().CoreV1().Pods(podInfo.Pod.GetNamespace()).Create(ctx, podInfo.Pod, metav1.CreateOptions{}); err != nil {
		//		klog.V(1).ErrorS(err, "migrate pod", klog.KObj(podInfo.Pod))
		//		return err
		//	}
		//}
		return fmt.Errorf("place pod failed")
	}

	klog.V(1).InfoS("migrate pod", "pod", klog.KObj(podInfo.Pod), "from", "pod", klog.KObj(nil), "to", "node", klog.KObj(toNodeInfo.Node()))
	err := scheduler.MigratePod(ctx, podEvictor, podInfo.Pod, nil, toNodeInfo.Node(), false)
	if err != nil {
		klog.V(1).ErrorS(err, "migrate pod", "pod", klog.KObj(podInfo.Pod))
		return err
	}

	return nil
}

func validateAndParsePlacePodsParams(params *api.StrategyParameters)error{
	if params == nil {
		return nil
	}

	if *params.Iterations < 0 {
		return fmt.Errorf("PlacePodsParams is empty")
	}

	// At most one of include/exclude can be set
	if params.Namespaces != nil && len(params.Namespaces.Include) > 0 && len(params.Namespaces.Exclude) > 0 {
		return fmt.Errorf("only one of Include/Exclude namespaces can be set")
	}

	return nil
}