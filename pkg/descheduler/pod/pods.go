/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod

import (
	"context"
	"sort"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/descheduler/pkg/utils"
)

type Options struct {
	filter             func(pod *v1.Pod) bool
	includedNamespaces []string
	excludedNamespaces []string
	labelSelector      *metav1.LabelSelector
}

// WithFilter sets a pod filter.
// The filter function should return true if the pod should be returned from ListPodsOnANode
func WithFilter(filter func(pod *v1.Pod) bool) func(opts *Options) {
	return func(opts *Options) {
		opts.filter = filter
	}
}

// WithNamespaces sets included namespaces
func WithNamespaces(namespaces []string) func(opts *Options) {
	return func(opts *Options) {
		opts.includedNamespaces = namespaces
	}
}

// WithoutNamespaces sets excluded namespaces
func WithoutNamespaces(namespaces []string) func(opts *Options) {
	return func(opts *Options) {
		opts.excludedNamespaces = namespaces
	}
}

// WithLabelSelector sets a pod label selector
func WithLabelSelector(labelSelector *metav1.LabelSelector) func(opts *Options) {
	return func(opts *Options) {
		opts.labelSelector = labelSelector
	}
}

// ListPodsOnANode lists all of the pods on a node
// It also accepts an optional "filter" function which can be used to further limit the pods that are returned.
// (Usually this is podEvictor.Evictable().IsEvictable, in order to only list the evictable pods on a node, but can
// be used by strategies to extend it if there are further restrictions, such as with NodeAffinity).
func ListPodsOnANode(
	ctx context.Context,
	client clientset.Interface,
	node *v1.Node,
	opts ...func(opts *Options),
) ([]*v1.Pod, error) {
	fieldSelectorString := "spec.nodeName=" + node.Name + ",status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed)

	return ListPodsOnANodeWithFieldSelector(ctx, client, node, fieldSelectorString, opts...)
}

// ListPodsOnANodeWithFieldSelector lists all of the pods on a node using the filter selectors
func ListPodsOnANodeWithFieldSelector(
	ctx context.Context,
	client clientset.Interface,
	node *v1.Node,
	fieldSelectorString string,
	opts ...func(opts *Options),
) ([]*v1.Pod, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	pods := make([]*v1.Pod, 0)

	labelSelectorString := ""
	if options.labelSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(options.labelSelector)
		if err != nil {
			return []*v1.Pod{}, err
		}
		labelSelectorString = selector.String()
	}

	if len(options.includedNamespaces) > 0 {
		fieldSelector, err := fields.ParseSelector(fieldSelectorString)
		if err != nil {
			return []*v1.Pod{}, err
		}

		for _, namespace := range options.includedNamespaces {
			podList, err := client.CoreV1().Pods(namespace).List(ctx,
				metav1.ListOptions{
					FieldSelector: fieldSelector.String(),
					LabelSelector: labelSelectorString,
				})
			if err != nil {
				return []*v1.Pod{}, err
			}
			for i := range podList.Items {
				if options.filter != nil && !options.filter(&podList.Items[i]) {
					continue
				}
				pods = append(pods, &podList.Items[i])
			}
		}
		return pods, nil
	}

	if len(options.excludedNamespaces) > 0 {
		for _, namespace := range options.excludedNamespaces {
			fieldSelectorString += ",metadata.namespace!=" + namespace
		}
	}

	fieldSelector, err := fields.ParseSelector(fieldSelectorString)
	if err != nil {
		return []*v1.Pod{}, err
	}

	// INFO(jchaloup): field selectors do not work properly with listers
	// Once the descheduler switches to pod listers (through informers),
	// We need to flip to client-side filtering.
	podList, err := client.CoreV1().Pods(v1.NamespaceAll).List(ctx,
		metav1.ListOptions{
			FieldSelector: fieldSelector.String(),
			LabelSelector: labelSelectorString,
		})
	if err != nil {
		return []*v1.Pod{}, err
	}

	for i := range podList.Items {
		// fake client does not support field selectors
		// so let's filter based on the node name as well (quite cheap)
		if podList.Items[i].Spec.NodeName != node.Name {
			continue
		}
		if options.filter != nil && !options.filter(&podList.Items[i]) {
			continue
		}
		pods = append(pods, &podList.Items[i])
	}
	return pods, nil
}

func ListPods(
	ctx context.Context,
	client clientset.Interface,
	opts ...func(opts *Options),
) ([]*v1.Pod, error) {
	fieldSelectorString := "status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed)

	return ListPodsWithFieldSelector(ctx, client, fieldSelectorString, opts...)
}

func ListPodsWithFieldSelector(
	ctx context.Context,
	client clientset.Interface,
	fieldSelectorString string,
	opts ...func(opts *Options),
) ([]*v1.Pod, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	pods := make([]*v1.Pod, 0)

	labelSelectorString := ""
	if options.labelSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(options.labelSelector)
		if err != nil {
			return []*v1.Pod{}, err
		}
		labelSelectorString = selector.String()
	}

	if len(options.includedNamespaces) > 0 {
		fieldSelector, err := fields.ParseSelector(fieldSelectorString)
		if err != nil {
			return []*v1.Pod{}, err
		}

		for _, namespace := range options.includedNamespaces {
			podList, err := client.CoreV1().Pods(namespace).List(ctx,
				metav1.ListOptions{
					FieldSelector: fieldSelector.String(),
					LabelSelector: labelSelectorString,
				})
			if err != nil {
				return []*v1.Pod{}, err
			}
			for i := range podList.Items {
				if options.filter != nil && !options.filter(&podList.Items[i]) {
					continue
				}
				pods = append(pods, &podList.Items[i])
			}
		}
		return pods, nil
	}

	if len(options.excludedNamespaces) > 0 {
		for _, namespace := range options.excludedNamespaces {
			fieldSelectorString += ",metadata.namespace!=" + namespace
		}
	}

	fieldSelector, err := fields.ParseSelector(fieldSelectorString)
	if err != nil {
		return []*v1.Pod{}, err
	}

	// INFO(jchaloup): field selectors do not work properly with listers
	// Once the descheduler switches to pod listers (through informers),
	// We need to flip to client-side filtering.
	podList, err := client.CoreV1().Pods(v1.NamespaceAll).List(ctx,
		metav1.ListOptions{
			FieldSelector: fieldSelector.String(),
			LabelSelector: labelSelectorString,
		})
	if err != nil {
		return []*v1.Pod{}, err
	}

	for i := range podList.Items {
		if options.filter != nil && !options.filter(&podList.Items[i]) {
			continue
		}
		pods = append(pods, &podList.Items[i])
	}
	return pods, nil
}


// OwnerRef returns the ownerRefList for the pod.
func OwnerRef(pod *v1.Pod) []metav1.OwnerReference {
	return pod.ObjectMeta.GetOwnerReferences()
}

func IsBestEffortPod(pod *v1.Pod) bool {
	return utils.GetPodQOS(pod) == v1.PodQOSBestEffort
}

func IsBurstablePod(pod *v1.Pod) bool {
	return utils.GetPodQOS(pod) == v1.PodQOSBurstable
}

func IsGuaranteedPod(pod *v1.Pod) bool {
	return utils.GetPodQOS(pod) == v1.PodQOSGuaranteed
}

// SortPodsBasedOnPriorityLowToHigh sorts pods based on their priorities from low to high.
// If pods have same priorities, they will be sorted by QoS in the following order:
// BestEffort, Burstable, Guaranteed
func SortPodsBasedOnPriorityLowToHigh(pods []*v1.Pod) {
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Spec.Priority == nil && pods[j].Spec.Priority != nil {
			return true
		}
		if pods[j].Spec.Priority == nil && pods[i].Spec.Priority != nil {
			return false
		}
		if (pods[j].Spec.Priority == nil && pods[i].Spec.Priority == nil) || (*pods[i].Spec.Priority == *pods[j].Spec.Priority) {
			if IsBestEffortPod(pods[i]) {
				return true
			}
			if IsBurstablePod(pods[i]) && IsGuaranteedPod(pods[j]) {
				return true
			}
			return false
		}
		return *pods[i].Spec.Priority < *pods[j].Spec.Priority
	})
}