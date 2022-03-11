package main

import (
	"context"
	"flag"
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/component-base/logs"
	"sigs.k8s.io/descheduler/pkg/descheduler/client"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/defragmentation/capacity"
)

const (
	balancePolicy = "balance"
	placePolicy = "place"
)

var (
	iterations int
	policy string
	podName string
	namespace string
	kubeconfig string
	nodeSelector string
)

func main(){
	flag.IntVar(&iterations, "iterations", 1, "balance pods on node iterations.")
	flag.StringVar(&policy, "policy", "", "place or balance.")
	flag.StringVar(&podName, "pod", "", "place pending pod name.")
	flag.StringVar(&namespace, "ns", "", "place pod namespace.")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kube config file.")
	flag.StringVar(&nodeSelector, "nodeSelector", "", "node selector.")
	flag.Parse()

	logs.InitLogs()
	defer logs.FlushLogs()

	if kubeconfig == "" {
		fmt.Printf("please input kubeconfig file")
		os.Exit(1)
	}

	stopChannel := make(chan struct{})
	ctx := context.Background()
	rsclient, err := client.CreateClient(kubeconfig)
	if err != nil {
		fmt.Printf("please input kubeconfig file")
		os.Exit(1)
	}

	sharedInformerFactory := informers.NewSharedInformerFactory(rsclient, 0)
	nodeInformer := sharedInformerFactory.Core().V1().Nodes()

	sharedInformerFactory.Start(stopChannel)
	sharedInformerFactory.WaitForCacheSync(stopChannel)

	nodes, err := nodeutil.ReadyNodes(ctx, rsclient, nodeInformer, nodeSelector)
	if err != nil {
		fmt.Printf("get ready nodes error:%s", err)
		os.Exit(1)
	}

	podEvictor := evictions.NewPodEvictor(
		rsclient,
		"",
		false,
		100,
		nodes,
		false,
		false,
		false,
	)

	if policy != placePolicy &&  policy != balancePolicy {
		klog.Errorf("please input valid policy: place or balance")
		os.Exit(1)
	}

	nodeInfos := capacity.GetSystemSnapshot(ctx, rsclient, nodes)

	if policy == placePolicy {
		if podName == "" {
			klog.Errorf("please provide the pending podName that has to be placed")
			os.Exit(1)
		}
		pod, err := rsclient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "failed to get pod", pod, klog.KObj(pod))
			return
		}
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
		podInfo := capacity.NewPodInfo(pod)
		if err := defragmentation.PlaceWorkload(ctx, podEvictor, podInfo, nodeInfos); err != nil {
			klog.ErrorS(err, "place pod across nodes", pod, klog.KObj(pod))
			return
		}

		time.Sleep(30 * time.Second)
		klog.V(1).Infoln("***********************************************************************************")
		nodeInfos = capacity.GetSystemSnapshot(ctx, rsclient, nodes)
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
	}else if policy == balancePolicy {
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
		klog.V(1).Infoln(" Nodes information before balancing :")
		for _, nodeInfo := range nodeInfos {
			klog.V(1).InfoS("-", "node", klog.KObj(nodeInfo.Node()))
			for _, podInfo := range nodeInfo.Pods {
				klog.V(1).InfoS("|-", "pod", klog.KObj(podInfo.Pod))
			}
		}

		currentIteration := 0
		for currentIteration < iterations {
			klog.V(1).Infof("This is the %d iteration", currentIteration+1)
			if err := defragmentation.BalanceWorkload(ctx, rsclient, nodes, podEvictor); err != nil {
				klog.ErrorS(err, "balance the cpu/memory consumption across nodes")
				currentIteration++
				continue
			}

			time.Sleep(30 * time.Second)
			klog.V(1).Infoln("***********************************************************************************")
			nodeInfos = capacity.GetSystemSnapshot(ctx, rsclient, nodes)
			totalCpu, totalMem, totalSr, usedCpu, usedMem, usedSr, availableCpu, availableMem, availableSr = capacity.GetNodeResourceUsage(nodeInfos...)
			klog.V(1).Infof("Nodes resource usage: total-cpu:%.3f, total-memory:%.3fMi, used-cpu:%.3f, used-memory:%.3fMi, available-cpu:%.3f, available-memory:%.3fMi", float64(totalCpu)/1000, float64(totalMem)/capacity.MB, float64(usedCpu)/1000, float64(usedMem)/capacity.MB, float64(availableCpu)/1000, float64(availableMem)/capacity.MB)
			for rName, rQuanta := range totalSr {
				klog.V(1).Infof("Nodes er usage: %s: total:%d, used:%d, available:%d", rName, rQuanta, usedSr[rName], availableSr[rName])
			}
			klog.V(1).Infof("Nodes usage: used-cpu(%%):%.2f%%, used-memory(%%):%.2f%%, fragment-cpu(%%):%.2f%%, fragment-memory(%%):%.2f%%", 100*float64(usedCpu)/float64(totalCpu), 100*float64(usedMem)/float64(totalMem), 100*float64(availableCpu)/float64(totalCpu), 100*float64(availableMem)/float64(totalMem))
			for rName, rQuanta := range availableSr {
				klog.V(1).Infof("Nodes er usage: %s used(%%):%.2f%%, fragment(%%):%.2f%%", rName, 100*float64(usedSr[rName])/float64(totalSr[rName]), 100*float64(rQuanta)/float64(totalSr[rName]))
			}
			klog.V(1).Infoln(" Nodes information after balancing :")
			for _, nodeInfo := range nodeInfos {
				klog.V(1).InfoS("-", "node", klog.KObj(nodeInfo.Node()))
				for _, podInfo := range nodeInfo.Pods {
					klog.V(1).InfoS(" |-", "pod", klog.KObj(podInfo.Pod))
				}
			}
			currentIteration++
		}
	}

	return
}
