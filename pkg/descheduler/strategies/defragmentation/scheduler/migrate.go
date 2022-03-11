package scheduler

import (
	"context"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/utils"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

func SwapPods(ctx context.Context, podEvictor *evictions.PodEvictor, podA *v1.Pod, nodeA *v1.Node, podB *v1.Pod, nodeB *v1.Node)error{
	//delete podA from nodeA
	//add podB to nodeA
	if err := MigratePod(ctx, podEvictor, podA, nodeA, nodeB, true); err != nil {
		klog.ErrorS(err, "migrate pod", "pod", klog.KObj(podA), "from_node", klog.KObj(nodeA), "to_node", klog.KObj(nodeB))
		return err
	}

	//delete podB from nodeB
	//add podA to nodeB
	if err := MigratePod(ctx, podEvictor, podB, nodeB, nodeA, true); err != nil {
		klog.ErrorS(err, "migrate pod", "pod", klog.KObj(podB), "from_node", klog.KObj(nodeB), "to_node", klog.KObj(nodeA))
		return err
	}

	// pod交换时, 副本控制器控制的副本延迟删除, 防止pod删除后, pod重新调度原节点
	for _, owner := range podB.GetOwnerReferences() {
		if owner.Kind == "ReplicationController" || owner.Kind == "ReplicaSet" {
			if err := deletePod(ctx, podEvictor, podB, nodeB); err != nil {
				klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(podB))
			}
			break
		}
	}

	for _, owner := range podA.GetOwnerReferences() {
		if owner.Kind == "ReplicationController" || owner.Kind == "ReplicaSet" {
			if err := deletePod(ctx, podEvictor, podA, nodeA); err != nil {
				klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(podB))
			}
			break
		}
	}

	return nil
}

type Controller struct {
	controllerType string
	Name	string
}

func MigratePod(ctx context.Context, podEvictor *evictions.PodEvictor, pod *v1.Pod, fromNode *v1.Node, toNode *v1.Node, isSwap bool)error {
	if fromNode == nil {
		return nil
	}

	var controller Controller
	for _, owner := range podutil.OwnerRef(pod) {
		if owner.Kind == "ReplicationController" {
			controller.controllerType = "ReplicationController"
			controller.Name = owner.Name
			break
		}else if owner.Kind == "ReplicaSet" {
			controller.controllerType = "ReplicaSet"
			controller.Name = owner.Name
			rs, err := podEvictor.Client().AppsV1().ReplicaSets(pod.GetNamespace()).Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil {
				klog.V(1).ErrorS(err, pod.GetNamespace(), pod.GetName(), "failed to get rs")
				break
			}
			for _, rsOwner := range rs.GetOwnerReferences() {
				if rsOwner.Kind == "Deployment" {
					controller.controllerType = "Deployment"
					controller.Name = rsOwner.Name
					break
				}
			}
			break
		}else if owner.Kind == "Job" {
			controller.controllerType = "Job"
			controller.Name = owner.Name
			break
		}else if owner.Kind == "TFJob" || owner.Kind == "PyTorchJob" || owner.Kind == "XGBoostJob" || owner.Kind == "MPIJob" || owner.Kind == "MXJob" || owner.Kind == "PaddleJob" {
			controller.controllerType = "Operator"
			controller.Name = pod.GetName()
			break
		}else {
			controller.controllerType = "Operator"
			controller.Name = pod.GetName()
			break
		}
	}

	//if controller.controllerType != "" {
	//	var cm v1.ConfigMap
	//	cm.Name = controller.Name
	//	cm.Data = make(map[string]string)
	//	cm.Data[cm.Name] = toNode.GetName()
	//	if _, err := podEvictor.Client().CoreV1().ConfigMaps(pod.GetNamespace()).Create(ctx, &cm, metav1.CreateOptions{}); err != nil {
	//		klog.ErrorS(err, "Error create configmap", "pod", klog.KObj(pod), "configmap", cm.Name)
	//	}
	//}

	if controller.controllerType == "" {
		klog.V(1).InfoS("delete pod", "pod",  klog.KObj(pod), "on node", klog.KObj(fromNode))
		if err := deletePod(ctx, podEvictor, pod, fromNode); err != nil {
			klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(pod))
			return err
		}
		klog.V(1).InfoS("reschedule pod", "pod",  klog.KObj(pod), "to node", klog.KObj(toNode))
		if err := reSchedulePod(ctx, podEvictor, pod, toNode.GetName(), isSwap); err != nil {
			klog.ErrorS(err, "Error reschedule pod", "pod", klog.KObj(pod))
			return err
		}
	}else if controller.controllerType == "ReplicationController" {
		 s, err := podEvictor.Client().CoreV1().ReplicationControllers(pod.GetNamespace()).GetScale(ctx, controller.Name, metav1.GetOptions{})
		 if err != nil {
			 klog.ErrorS(err, "Error get pod controller rc scale", "pod", klog.KObj(pod))
			 return err
		 }
		sc := *s
		sc.Spec.Replicas +=1
		if _, err := podEvictor.Client().CoreV1().ReplicationControllers(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil{
			klog.ErrorS(err, "Error update pod controller rc scale", "pod", klog.KObj(pod))
			return err
		}

		if !isSwap {
			if err := deletePod(ctx, podEvictor, pod, fromNode); err != nil {
				klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(pod))
				return err
			}
		}

		sc.ResourceVersion = ""
		sc.Spec.Replicas -=1
		if _, err := podEvictor.Client().CoreV1().ReplicationControllers(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil{
			klog.ErrorS(err, "Error update pod rc", "pod", klog.KObj(pod))
			return err
		}
	}else if controller.controllerType == "ReplicaSet" {
		s, err := podEvictor.Client().AppsV1().ReplicaSets(pod.GetNamespace()).GetScale(ctx, controller.Name, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Error get pod controller rs scale", "pod", klog.KObj(pod))
			return err
		}
		sc := *s
		sc.Spec.Replicas +=1
		if _, err := podEvictor.Client().AppsV1().ReplicaSets(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil{
			klog.ErrorS(err, "Error update pod controller rs scale", "pod", klog.KObj(pod))
			return err
		}
		if !isSwap {
			if err := deletePod(ctx, podEvictor, pod, fromNode); err != nil {
				klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(pod))
				return err
			}
		}
		sc.ResourceVersion = ""
		sc.Spec.Replicas -=1
		if _, err := podEvictor.Client().AppsV1().ReplicaSets(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil{
			klog.ErrorS(err, "Error update pod controller rs scale", "pod", klog.KObj(pod))
			return err
		}
	}else if controller.controllerType == "Deployment"{
		s, err := podEvictor.Client().AppsV1().Deployments(pod.GetNamespace()).GetScale(ctx, controller.Name, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "Error get pod deployment", "pod", klog.KObj(pod))
			return err
		}
		sc := *s
		sc.Spec.Replicas += 1
		if _, err := podEvictor.Client().AppsV1().Deployments(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil {
			klog.ErrorS(err, "Error update pod controller deployment scale", "pod", klog.KObj(pod))
			return err
		}
		if !isSwap {
			if err := deletePod(ctx, podEvictor, pod, fromNode); err != nil {
				klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(pod))
				return err
			}
		}

		sc.ResourceVersion = ""
		sc.Spec.Replicas -= 1
		if _, err := podEvictor.Client().AppsV1().Deployments(pod.GetNamespace()).UpdateScale(ctx, controller.Name, &sc, metav1.UpdateOptions{}); err != nil {
			klog.ErrorS(err, "Error update pod controller deployment scale", "pod", klog.KObj(pod))
			return err
		}
	}else if controller.controllerType == "Job" || controller.controllerType == "Operator"{
		if err := deletePod(ctx, podEvictor, pod, fromNode); err != nil {
			klog.ErrorS(err, "Error delete pod", "pod", klog.KObj(pod))
			return err
		}
	}

	return nil
}

func reSchedulePod(ctx context.Context, podEvictor *evictions.PodEvictor, pod *v1.Pod, toNode string, isSwap bool)error{
	schedulePod := pod.DeepCopy()
	schedulePod.SetResourceVersion("")
	schedulePod.UID = ""
	schedulePod.Spec.NodeName = ""
	if isSwap {
		utils.SetNewPreferredPodAffinity(schedulePod, toNode)
	}
	schedulePod.Status.Reset()

	if _, err := podEvictor.Client().CoreV1().Pods(schedulePod.Namespace).Create(ctx, schedulePod, metav1.CreateOptions{}); err != nil {
		klog.V(1).ErrorS(err, "Error reschedule pod", klog.KObj(pod), toNode)
		return err
	}

	return nil
}

func deletePod(ctx context.Context, podEvictor *evictions.PodEvictor, pod *v1.Pod, node *v1.Node)error{
	gracePeriodSeconds := int64(0)
	if err := podEvictor.Client().CoreV1().Pods(pod.GetNamespace()).Delete(ctx, pod.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds}); err != nil {
	//if _, err := podEvictor.EvictPod(ctx, pod, node, "defragment"); err != nil {
		klog.ErrorS(err, "Error evicting pod", klog.KObj(pod))
		return err
	}

	return nil
}


