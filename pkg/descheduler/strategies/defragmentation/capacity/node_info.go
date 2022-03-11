package capacity

import (
	"fmt"
	"math"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sync/atomic"
)

const (
	KB = 1024
	MB = 1024 * KB
)

var generation int64

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	// Overall node information.
	node *v1.Node

	// Pods running on the node.
	Pods []*PodInfo

	// The subset of pods with affinity.
	PodsWithAffinity []*PodInfo

	// The subset of pods with required anti-affinity.
	PodsWithRequiredAntiAffinity []*PodInfo

	// Total requested resources of all pods on this node. This includes assumed
	// pods, which scheduler has sent for binding, but may not be scheduled yet.
	Requested *Resource
	// Total requested resources of all pods on this node with a minimum value
	// applied to each container's CPU and memory requests. This does not reflect
	// the actual resource requests for this node, but is used to avoid scheduling
	// many zero-request pods onto one node.
	NonZeroRequested *Resource
	// We store allocatedResources (which is Node.Status.Allocatable.*) explicitly
	// as int64, to avoid conversions and accessing map.
	Allocatable *Resource

	Available *Resource

	// Whenever NodeInfo changes, generation is bumped.
	// This is used to avoid cloning it if the object didn't change.
	Generation int64
}

// nextGeneration: Let's make sure history never forgets the name...
// Increments the generation number monotonically ensuring that generation numbers never collide.
// Collision of the generation numbers would be particularly problematic if a node was deleted and
// added back with the same name. See issue#63262.
func nextGeneration() int64 {
	return atomic.AddInt64(&generation, 1)
}

// NewNodeInfo returns a ready to use empty NodeInfo object.
// If any pods are given in arguments, their information will be aggregated in
// the returned object.
func NewNodeInfo(pods ...*v1.Pod) *NodeInfo {
	ni := &NodeInfo{
		Requested:        &Resource{},
		NonZeroRequested: &Resource{},
		Allocatable:      &Resource{},
		Available:        &Resource{},
		Generation:       nextGeneration(),
	}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

// Node returns overall information about this node.
func (n *NodeInfo) Node() *v1.Node {
	if n == nil {
		return nil
	}
	return n.node
}

// Clone returns a copy of this node.
func (n *NodeInfo) Clone() *NodeInfo {
	clone := &NodeInfo{
		node:             n.node,
		Requested:        n.Requested.Clone(),
		NonZeroRequested: n.NonZeroRequested.Clone(),
		Allocatable:      n.Allocatable.Clone(),
		Available: 		  n.Available.Clone(),
		Generation:       n.Generation,
	}
	if len(n.Pods) > 0 {
		clone.Pods = append([]*PodInfo(nil), n.Pods...)
	}

	if len(n.PodsWithAffinity) > 0 {
		clone.PodsWithAffinity = append([]*PodInfo(nil), n.PodsWithAffinity...)
	}
	if len(n.PodsWithRequiredAntiAffinity) > 0 {
		clone.PodsWithRequiredAntiAffinity = append([]*PodInfo(nil), n.PodsWithRequiredAntiAffinity...)
	}
	return clone
}

// String returns representation of human readable format of this NodeInfo.
func (n *NodeInfo) String() string {
	podKeys := make([]string, len(n.Pods))
	for i, p := range n.Pods {
		podKeys[i] = p.Pod.Name
	}
	return fmt.Sprintf("&NodeInfo{Pods:%v, RequestedResource:%#v, NonZeroRequest: %#v, AllocatableResource:%#v}",
		podKeys, n.Requested, n.NonZeroRequested, n.Allocatable)
}

// AddPodInfo adds pod information to this NodeInfo.
// Consider using this instead of AddPod if a PodInfo is already computed.
func (n *NodeInfo) AddPodInfo(podInfo *PodInfo) {
	res, non0CPU, non0Mem := CalculateResource(podInfo.Pod)
	n.Requested.MilliCPU += res.MilliCPU
	n.Requested.Memory += res.Memory
	if n.Requested.ScalarResources == nil {
		n.Requested.ScalarResources = map[v1.ResourceName]int64{}
	}
	for rName, rQuant := range res.ScalarResources {
		n.Requested.ScalarResources[rName] += rQuant
	}
	n.NonZeroRequested.MilliCPU += non0CPU
	n.NonZeroRequested.Memory += non0Mem
	if n.NonZeroRequested.ScalarResources == nil {
		n.NonZeroRequested.ScalarResources = make(map[v1.ResourceName]int64)
	}
	for rName, rQuant := range res.ScalarResources {
		n.NonZeroRequested.ScalarResources[rName] += rQuant
	}
	n.Available.MilliCPU -= non0CPU
	n.Available.Memory -= non0Mem
	if n.Available.ScalarResources == nil {
		n.Available.ScalarResources = make(map[v1.ResourceName]int64)
	}
	for rName, rQuant := range res.ScalarResources {
		n.Available.ScalarResources[rName] -= rQuant
	}
	n.Pods = append(n.Pods, podInfo)
	if podWithAffinity(podInfo.Pod) {
		n.PodsWithAffinity = append(n.PodsWithAffinity, podInfo)
	}
	if podWithRequiredAntiAffinity(podInfo.Pod) {
		n.PodsWithRequiredAntiAffinity = append(n.PodsWithRequiredAntiAffinity, podInfo)
	}

	n.Generation = nextGeneration()
}

// AddPod is a wrapper around AddPodInfo.
func (n *NodeInfo) AddPod(pod *v1.Pod) {
	n.AddPodInfo(NewPodInfo(pod))
}

func removeFromSlice(s []*PodInfo, k string) []*PodInfo {
	for i := range s {
		k2, err := GetPodKey(s[i].Pod)
		if err != nil {
			klog.ErrorS(err, "Cannot get pod key", "pod", klog.KObj(s[i].Pod))
			continue
		}
		if k == k2 {
			// delete the element
			s[i] = s[len(s)-1]
			s = s[:len(s)-1]
			break
		}
	}
	return s
}

// RemovePod subtracts pod information from this NodeInfo.
func (n *NodeInfo) RemovePod(pod *v1.Pod) error {
	k, err := GetPodKey(pod)
	if err != nil {
		return err
	}
	if podWithAffinity(pod) {
		n.PodsWithAffinity = removeFromSlice(n.PodsWithAffinity, k)
	}
	if podWithRequiredAntiAffinity(pod) {
		n.PodsWithRequiredAntiAffinity = removeFromSlice(n.PodsWithRequiredAntiAffinity, k)
	}

	for i := range n.Pods {
		k2, err := GetPodKey(n.Pods[i].Pod)
		if err != nil {
			klog.ErrorS(err, "Cannot get pod key", "pod", klog.KObj(n.Pods[i].Pod))
			continue
		}
		if k == k2 {
			// delete the element
			n.Pods[i] = n.Pods[len(n.Pods)-1]
			n.Pods = n.Pods[:len(n.Pods)-1]
			// reduce the resource data
			res, non0CPU, non0Mem := CalculateResource(pod)

			n.Requested.MilliCPU -= res.MilliCPU
			n.Requested.Memory -= res.Memory
			if len(res.ScalarResources) > 0 && n.Requested.ScalarResources == nil {
				n.Requested.ScalarResources = map[v1.ResourceName]int64{}
			}
			for rName, rQuant := range res.ScalarResources {
				n.Requested.ScalarResources[rName] -= rQuant
			}
			n.NonZeroRequested.MilliCPU -= non0CPU
			n.NonZeroRequested.Memory -= non0Mem
			n.Available.MilliCPU += non0CPU
			n.Available.Memory += non0Mem
			n.Generation = nextGeneration()
			n.resetSlicesIfEmpty()
			return nil
		}
	}
	return fmt.Errorf("no corresponding pod %s in pods of node %s", pod.Name, n.node.Name)
}

// resets the slices to nil so that we can do DeepEqual in unit tests.
func (n *NodeInfo) resetSlicesIfEmpty() {
	if len(n.PodsWithAffinity) == 0 {
		n.PodsWithAffinity = nil
	}
	if len(n.PodsWithRequiredAntiAffinity) == 0 {
		n.PodsWithRequiredAntiAffinity = nil
	}
	if len(n.Pods) == 0 {
		n.Pods = nil
	}
}

// SetNode sets the overall node information.
func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Allocatable = NewResource(node.Status.Allocatable)
	n.Available = NewResource(node.Status.Allocatable)
	n.Generation = nextGeneration()
}

// RemoveNode removes the node object, leaving all other tracking information.
func (n *NodeInfo) RemoveNode() {
	n.node = nil
	n.Generation = nextGeneration()
}

// FilterOutPods receives a list of pods and filters out those whose node names
// are equal to the node of this NodeInfo, but are not found in the pods of this NodeInfo.
//
// Preemption logic simulates removal of pods on a node by removing them from the
// corresponding NodeInfo. In order for the simulation to work, we call this method
// on the pods returned from SchedulerCache, so that predicate functions see
// only the pods that are not removed from the NodeInfo.
func (n *NodeInfo) FilterOutPods(pods []*v1.Pod) []*v1.Pod {
	node := n.Node()
	if node == nil {
		return pods
	}
	filtered := make([]*v1.Pod, 0, len(pods))
	for _, p := range pods {
		if p.Spec.NodeName != node.Name {
			filtered = append(filtered, p)
			continue
		}
		// If pod is on the given node, add it to 'filtered' only if it is present in nodeInfo.
		podKey, err := GetPodKey(p)
		if err != nil {
			continue
		}
		for _, np := range n.Pods {
			npodkey, _ := GetPodKey(np.Pod)
			if npodkey == podKey {
				filtered = append(filtered, p)
				break
			}
		}
	}
	return filtered
}

func SortNodesBasedRatio(nodeInfos []*NodeInfo) {
	sort.Slice(nodeInfos, func(i, j int) bool {
		return GetCpuMemoryRatio(nodeInfos[i]) < GetCpuMemoryRatio(nodeInfos[j])
	})
}


func GetDistanceFromPivot(nodeInfo *NodeInfo, pivotratio float64)float64{
	return math.Abs(pivotratio - GetCpuMemoryRatio(nodeInfo))
}

func GetSystemEntropy(nodeInfos []*NodeInfo)float64{
	var sum float64

	pivotRatio := GetPivotRatio(nodeInfos)
	for _, nodeInfo := range nodeInfos {
		sum += math.Abs(pivotRatio - GetCpuMemoryRatio(nodeInfo))
	}

	return sum
}

func GetPivotRatio(nodeInfos []*NodeInfo)float64 {
	var totalAvailableCpu, totalAvailableMem int64
	for _, nodeInfo := range nodeInfos {
		totalAvailableCpu += nodeInfo.Available.MilliCPU
		totalAvailableMem += nodeInfo.Available.Memory
	}

	availableMem := int64(float64(totalAvailableMem) / MB)

	return float64(totalAvailableCpu)/float64(availableMem)
}

func GetCpuMemoryRatio(nodeInfo *NodeInfo)float64{
	if int64(float64(nodeInfo.Available.Memory)/MB) == 0 {
		return 1000000
	}

	if nodeInfo.Available.MilliCPU == 0 {
		return 0
	}

	return float64(nodeInfo.Available.MilliCPU) / (float64(nodeInfo.Available.Memory)/MB)
}

func GetStatus(nodeInfos []*NodeInfo){
	klog.V(5).Infoln("\n\n******************* System Availability Status ********************\n\n")
	for  _, nodeInfo := range nodeInfos {
		klog.V(5).InfoS("Node : %s  Available %#v", nodeInfo.node.GetName(), nodeInfo.node.Status.Capacity)
	}

	return
}