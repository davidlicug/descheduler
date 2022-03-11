package utils

import (
	v1 "k8s.io/api/core/v1"
	"strings"
)

const (
	nodeAffinityHostNameKey = "kubernetes.io/hostname"
)

func SetNewRequiredPodAffinity(pod *v1.Pod, toNode string){
	if pod == nil {
		return
	}

	affinity := pod.Spec.Affinity
	if affinity == nil {
		affinity = &v1.Affinity{}
		pod.Spec.Affinity = affinity
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		nodeAffinity = &v1.NodeAffinity{}
		affinity.NodeAffinity = nodeAffinity
	}

	// 增加节点亲和性，将pod定向调度到指定node
	requiredDuringSchedulingIgnoredDuringExecution := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if requiredDuringSchedulingIgnoredDuringExecution == nil {
		requiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = requiredDuringSchedulingIgnoredDuringExecution
	}

	if len(requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms  = append(requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, v1.NodeSelectorTerm{})
	}

	requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[len(requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) - 1].MatchExpressions = append(requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[len(requiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) - 1].MatchExpressions, v1.NodeSelectorRequirement{
		Key: nodeAffinityHostNameKey,
		Operator: v1.NodeSelectorOpIn,
		Values: []string{toNode},
	})

	return
}

func SetNewPreferredPodAffinity(pod *v1.Pod, toNode string){
	if pod == nil {
		return
	}

	affinity := pod.Spec.Affinity
	if affinity == nil {
		affinity = &v1.Affinity{}
		pod.Spec.Affinity = affinity
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		nodeAffinity = &v1.NodeAffinity{}
		affinity.NodeAffinity = nodeAffinity
	}

	// 增加节点亲和性，将pod定向调度到指定node
	if len(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, v1.PreferredSchedulingTerm{Weight:1})
	}

	nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[len(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)-1].Preference.MatchExpressions = append(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[len(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)-1].Preference.MatchExpressions, v1.NodeSelectorRequirement{
		Key: nodeAffinityHostNameKey,
		Operator: v1.NodeSelectorOpIn,
		Values: []string{toNode},
	})

	return
}

func IsPendingPod(pod *v1.Pod)bool{
	if pod.Spec.NodeName == "" && pod.Status.Phase == "Pending" {
		for _, condition := range pod.Status.Conditions {
			if strings.Contains(condition.Message, "nodes are available") {
				return true
			}
		}
	}

	return false
}

func IsSpecifyPriorityPod(pod *v1.Pod, priority int32)bool{
	if *pod.Spec.Priority == priority {
		return true
	}
	return false
}