package capacity

import (
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

type PodInfo struct {
	Pod                        *v1.Pod
	RequiredAffinityTerms      []AffinityTerm
	RequiredAntiAffinityTerms  []AffinityTerm
	PreferredAffinityTerms     []WeightedAffinityTerm
	PreferredAntiAffinityTerms []WeightedAffinityTerm
	ParseError                 error
}

// NewPodInfo returns a new PodInfo.
func NewPodInfo(pod *v1.Pod) *PodInfo {
	pInfo := &PodInfo{}
	pInfo.Update(pod)
	return pInfo
}

// DeepCopy returns a deep copy of the PodInfo object.
func (pi *PodInfo) DeepCopy() *PodInfo {
	return &PodInfo{
		Pod:                        pi.Pod.DeepCopy(),
		RequiredAffinityTerms:      pi.RequiredAffinityTerms,
		RequiredAntiAffinityTerms:  pi.RequiredAntiAffinityTerms,
		PreferredAffinityTerms:     pi.PreferredAffinityTerms,
		PreferredAntiAffinityTerms: pi.PreferredAntiAffinityTerms,
		ParseError:                 pi.ParseError,
	}
}

// Update creates a full new PodInfo by default. And only updates the pod when the PodInfo
// has been instantiated and the passed pod is the exact same one as the original pod.
func (pi *PodInfo) Update(pod *v1.Pod) {
	if pod != nil && pi.Pod != nil && pi.Pod.UID == pod.UID {
		// PodInfo includes immutable information, and so it is safe to update the pod in place if it is
		// the exact same pod
		pi.Pod = pod
		return
	}
	var preferredAffinityTerms []v1.WeightedPodAffinityTerm
	var preferredAntiAffinityTerms []v1.WeightedPodAffinityTerm
	if affinity := pod.Spec.Affinity; affinity != nil {
		if a := affinity.PodAffinity; a != nil {
			preferredAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
		if a := affinity.PodAntiAffinity; a != nil {
			preferredAntiAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
	}

	// Attempt to parse the affinity terms
	var parseErrs []error
	requiredAffinityTerms, err := getAffinityTerms(pod, getPodAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAffinityTerms: %w", err))
	}
	requiredAntiAffinityTerms, err := getAffinityTerms(pod,
		getPodAntiAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAntiAffinityTerms: %w", err))
	}
	weightedAffinityTerms, err := getWeightedAffinityTerms(pod, preferredAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAffinityTerms: %w", err))
	}
	weightedAntiAffinityTerms, err := getWeightedAffinityTerms(pod, preferredAntiAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAntiAffinityTerms: %w", err))
	}

	pi.Pod = pod
	pi.RequiredAffinityTerms = requiredAffinityTerms
	pi.RequiredAntiAffinityTerms = requiredAntiAffinityTerms
	pi.PreferredAffinityTerms = weightedAffinityTerms
	pi.PreferredAntiAffinityTerms = weightedAntiAffinityTerms
	pi.ParseError = utilerrors.NewAggregate(parseErrs)
}

func SortPodsBasedOnCpu(podInfos []*PodInfo)[]*PodInfo{
	var sortPodInfos []*PodInfo
	sortPodInfos = append(sortPodInfos, podInfos...)
	sort.Slice(podInfos, func(i, j int) bool {
		return getCpuRequests(podInfos[i].Pod) < getCpuRequests(podInfos[j].Pod)
	})

	return sortPodInfos
}

func SortPodsBasedOnMemory(podInfos []*PodInfo)[]*PodInfo{
	var sortPodInfos []*PodInfo
	sortPodInfos = append(sortPodInfos, podInfos...)
	sort.Slice(podInfos, func(i, j int) bool {
		return getMemoryRequests(podInfos[i].Pod) < getMemoryRequests(podInfos[j].Pod)
	})

	return sortPodInfos
}

func getCpuRequests(pod *v1.Pod)int64{
	var non0CPU int64
	for _, c := range pod.Spec.Containers {
		non0CPUReq, _ := getNonzeroRequests(&c.Resources.Requests)
		non0CPU += non0CPUReq
	}

	for _, ic := range pod.Spec.InitContainers {
		non0CPUReq, _ := getNonzeroRequests(&ic.Resources.Requests)
		non0CPU = max(non0CPU, non0CPUReq)
	}
	return non0CPU
}

func getMemoryRequests(pod *v1.Pod)int64{
	var non0Mem int64
	for _, c := range pod.Spec.Containers {
		_, non0MemReq := getNonzeroRequests(&c.Resources.Requests)
		non0Mem += non0MemReq
		// No non-zero resources for GPUs or opaque resources.
	}

	for _, ic := range pod.Spec.InitContainers {
		_, non0MemReq := getNonzeroRequests(&ic.Resources.Requests)
		non0Mem = max(non0Mem, non0MemReq)
	}

	return non0Mem
}