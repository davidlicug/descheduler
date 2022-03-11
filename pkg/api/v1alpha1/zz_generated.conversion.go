//go:build !ignore_autogenerated
// +build !ignore_autogenerated

/*
Copyright 2021 The Kubernetes Authors.

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

// Code generated by conversion-gen. DO NOT EDIT.

package v1alpha1

import (
	unsafe "unsafe"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	runtime "k8s.io/apimachinery/pkg/runtime"
	api "sigs.k8s.io/descheduler/pkg/api"
)

func init() {
	localSchemeBuilder.Register(RegisterConversions)
}

// RegisterConversions adds conversion functions to the given scheme.
// Public to allow building arbitrary schemes.
func RegisterConversions(s *runtime.Scheme) error {
	if err := s.AddGeneratedConversionFunc((*DeschedulerPolicy)(nil), (*api.DeschedulerPolicy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_DeschedulerPolicy_To_api_DeschedulerPolicy(a.(*DeschedulerPolicy), b.(*api.DeschedulerPolicy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.DeschedulerPolicy)(nil), (*DeschedulerPolicy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_DeschedulerPolicy_To_v1alpha1_DeschedulerPolicy(a.(*api.DeschedulerPolicy), b.(*DeschedulerPolicy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*DeschedulerStrategy)(nil), (*api.DeschedulerStrategy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_DeschedulerStrategy_To_api_DeschedulerStrategy(a.(*DeschedulerStrategy), b.(*api.DeschedulerStrategy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.DeschedulerStrategy)(nil), (*DeschedulerStrategy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_DeschedulerStrategy_To_v1alpha1_DeschedulerStrategy(a.(*api.DeschedulerStrategy), b.(*DeschedulerStrategy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*FailedPods)(nil), (*api.FailedPods)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_FailedPods_To_api_FailedPods(a.(*FailedPods), b.(*api.FailedPods), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.FailedPods)(nil), (*FailedPods)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_FailedPods_To_v1alpha1_FailedPods(a.(*api.FailedPods), b.(*FailedPods), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*Namespaces)(nil), (*api.Namespaces)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_Namespaces_To_api_Namespaces(a.(*Namespaces), b.(*api.Namespaces), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.Namespaces)(nil), (*Namespaces)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_Namespaces_To_v1alpha1_Namespaces(a.(*api.Namespaces), b.(*Namespaces), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*NodeResourceUtilizationThresholds)(nil), (*api.NodeResourceUtilizationThresholds)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_NodeResourceUtilizationThresholds_To_api_NodeResourceUtilizationThresholds(a.(*NodeResourceUtilizationThresholds), b.(*api.NodeResourceUtilizationThresholds), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.NodeResourceUtilizationThresholds)(nil), (*NodeResourceUtilizationThresholds)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_NodeResourceUtilizationThresholds_To_v1alpha1_NodeResourceUtilizationThresholds(a.(*api.NodeResourceUtilizationThresholds), b.(*NodeResourceUtilizationThresholds), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*PodLifeTime)(nil), (*api.PodLifeTime)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_PodLifeTime_To_api_PodLifeTime(a.(*PodLifeTime), b.(*api.PodLifeTime), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.PodLifeTime)(nil), (*PodLifeTime)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_PodLifeTime_To_v1alpha1_PodLifeTime(a.(*api.PodLifeTime), b.(*PodLifeTime), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*PodsHavingTooManyRestarts)(nil), (*api.PodsHavingTooManyRestarts)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_PodsHavingTooManyRestarts_To_api_PodsHavingTooManyRestarts(a.(*PodsHavingTooManyRestarts), b.(*api.PodsHavingTooManyRestarts), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.PodsHavingTooManyRestarts)(nil), (*PodsHavingTooManyRestarts)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_PodsHavingTooManyRestarts_To_v1alpha1_PodsHavingTooManyRestarts(a.(*api.PodsHavingTooManyRestarts), b.(*PodsHavingTooManyRestarts), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*RemoveDuplicates)(nil), (*api.RemoveDuplicates)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_RemoveDuplicates_To_api_RemoveDuplicates(a.(*RemoveDuplicates), b.(*api.RemoveDuplicates), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.RemoveDuplicates)(nil), (*RemoveDuplicates)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_RemoveDuplicates_To_v1alpha1_RemoveDuplicates(a.(*api.RemoveDuplicates), b.(*RemoveDuplicates), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*StrategyParameters)(nil), (*api.StrategyParameters)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha1_StrategyParameters_To_api_StrategyParameters(a.(*StrategyParameters), b.(*api.StrategyParameters), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.StrategyParameters)(nil), (*StrategyParameters)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_StrategyParameters_To_v1alpha1_StrategyParameters(a.(*api.StrategyParameters), b.(*StrategyParameters), scope)
	}); err != nil {
		return err
	}
	return nil
}

func autoConvert_v1alpha1_DeschedulerPolicy_To_api_DeschedulerPolicy(in *DeschedulerPolicy, out *api.DeschedulerPolicy, s conversion.Scope) error {
	out.Strategies = *(*api.StrategyList)(unsafe.Pointer(&in.Strategies))
	out.NodeSelector = (*string)(unsafe.Pointer(in.NodeSelector))
	out.EvictLocalStoragePods = (*bool)(unsafe.Pointer(in.EvictLocalStoragePods))
	out.EvictSystemCriticalPods = (*bool)(unsafe.Pointer(in.EvictSystemCriticalPods))
	out.IgnorePVCPods = (*bool)(unsafe.Pointer(in.IgnorePVCPods))
	out.MaxNoOfPodsToEvictPerNode = (*int)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNode))
	return nil
}

// Convert_v1alpha1_DeschedulerPolicy_To_api_DeschedulerPolicy is an autogenerated conversion function.
func Convert_v1alpha1_DeschedulerPolicy_To_api_DeschedulerPolicy(in *DeschedulerPolicy, out *api.DeschedulerPolicy, s conversion.Scope) error {
	return autoConvert_v1alpha1_DeschedulerPolicy_To_api_DeschedulerPolicy(in, out, s)
}

func autoConvert_api_DeschedulerPolicy_To_v1alpha1_DeschedulerPolicy(in *api.DeschedulerPolicy, out *DeschedulerPolicy, s conversion.Scope) error {
	out.Strategies = *(*StrategyList)(unsafe.Pointer(&in.Strategies))
	out.NodeSelector = (*string)(unsafe.Pointer(in.NodeSelector))
	out.EvictLocalStoragePods = (*bool)(unsafe.Pointer(in.EvictLocalStoragePods))
	out.EvictSystemCriticalPods = (*bool)(unsafe.Pointer(in.EvictSystemCriticalPods))
	out.IgnorePVCPods = (*bool)(unsafe.Pointer(in.IgnorePVCPods))
	out.MaxNoOfPodsToEvictPerNode = (*int)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNode))
	return nil
}

// Convert_api_DeschedulerPolicy_To_v1alpha1_DeschedulerPolicy is an autogenerated conversion function.
func Convert_api_DeschedulerPolicy_To_v1alpha1_DeschedulerPolicy(in *api.DeschedulerPolicy, out *DeschedulerPolicy, s conversion.Scope) error {
	return autoConvert_api_DeschedulerPolicy_To_v1alpha1_DeschedulerPolicy(in, out, s)
}

func autoConvert_v1alpha1_DeschedulerStrategy_To_api_DeschedulerStrategy(in *DeschedulerStrategy, out *api.DeschedulerStrategy, s conversion.Scope) error {
	out.Enabled = in.Enabled
	out.Weight = in.Weight
	out.Params = (*api.StrategyParameters)(unsafe.Pointer(in.Params))
	return nil
}

// Convert_v1alpha1_DeschedulerStrategy_To_api_DeschedulerStrategy is an autogenerated conversion function.
func Convert_v1alpha1_DeschedulerStrategy_To_api_DeschedulerStrategy(in *DeschedulerStrategy, out *api.DeschedulerStrategy, s conversion.Scope) error {
	return autoConvert_v1alpha1_DeschedulerStrategy_To_api_DeschedulerStrategy(in, out, s)
}

func autoConvert_api_DeschedulerStrategy_To_v1alpha1_DeschedulerStrategy(in *api.DeschedulerStrategy, out *DeschedulerStrategy, s conversion.Scope) error {
	out.Enabled = in.Enabled
	out.Weight = in.Weight
	out.Params = (*StrategyParameters)(unsafe.Pointer(in.Params))
	return nil
}

// Convert_api_DeschedulerStrategy_To_v1alpha1_DeschedulerStrategy is an autogenerated conversion function.
func Convert_api_DeschedulerStrategy_To_v1alpha1_DeschedulerStrategy(in *api.DeschedulerStrategy, out *DeschedulerStrategy, s conversion.Scope) error {
	return autoConvert_api_DeschedulerStrategy_To_v1alpha1_DeschedulerStrategy(in, out, s)
}

func autoConvert_v1alpha1_FailedPods_To_api_FailedPods(in *FailedPods, out *api.FailedPods, s conversion.Scope) error {
	out.ExcludeOwnerKinds = *(*[]string)(unsafe.Pointer(&in.ExcludeOwnerKinds))
	out.MinPodLifetimeSeconds = (*uint)(unsafe.Pointer(in.MinPodLifetimeSeconds))
	out.Reasons = *(*[]string)(unsafe.Pointer(&in.Reasons))
	out.IncludingInitContainers = in.IncludingInitContainers
	return nil
}

// Convert_v1alpha1_FailedPods_To_api_FailedPods is an autogenerated conversion function.
func Convert_v1alpha1_FailedPods_To_api_FailedPods(in *FailedPods, out *api.FailedPods, s conversion.Scope) error {
	return autoConvert_v1alpha1_FailedPods_To_api_FailedPods(in, out, s)
}

func autoConvert_api_FailedPods_To_v1alpha1_FailedPods(in *api.FailedPods, out *FailedPods, s conversion.Scope) error {
	out.ExcludeOwnerKinds = *(*[]string)(unsafe.Pointer(&in.ExcludeOwnerKinds))
	out.MinPodLifetimeSeconds = (*uint)(unsafe.Pointer(in.MinPodLifetimeSeconds))
	out.Reasons = *(*[]string)(unsafe.Pointer(&in.Reasons))
	out.IncludingInitContainers = in.IncludingInitContainers
	return nil
}

// Convert_api_FailedPods_To_v1alpha1_FailedPods is an autogenerated conversion function.
func Convert_api_FailedPods_To_v1alpha1_FailedPods(in *api.FailedPods, out *FailedPods, s conversion.Scope) error {
	return autoConvert_api_FailedPods_To_v1alpha1_FailedPods(in, out, s)
}

func autoConvert_v1alpha1_Namespaces_To_api_Namespaces(in *Namespaces, out *api.Namespaces, s conversion.Scope) error {
	out.Include = *(*[]string)(unsafe.Pointer(&in.Include))
	out.Exclude = *(*[]string)(unsafe.Pointer(&in.Exclude))
	return nil
}

// Convert_v1alpha1_Namespaces_To_api_Namespaces is an autogenerated conversion function.
func Convert_v1alpha1_Namespaces_To_api_Namespaces(in *Namespaces, out *api.Namespaces, s conversion.Scope) error {
	return autoConvert_v1alpha1_Namespaces_To_api_Namespaces(in, out, s)
}

func autoConvert_api_Namespaces_To_v1alpha1_Namespaces(in *api.Namespaces, out *Namespaces, s conversion.Scope) error {
	out.Include = *(*[]string)(unsafe.Pointer(&in.Include))
	out.Exclude = *(*[]string)(unsafe.Pointer(&in.Exclude))
	return nil
}

// Convert_api_Namespaces_To_v1alpha1_Namespaces is an autogenerated conversion function.
func Convert_api_Namespaces_To_v1alpha1_Namespaces(in *api.Namespaces, out *Namespaces, s conversion.Scope) error {
	return autoConvert_api_Namespaces_To_v1alpha1_Namespaces(in, out, s)
}

func autoConvert_v1alpha1_NodeResourceUtilizationThresholds_To_api_NodeResourceUtilizationThresholds(in *NodeResourceUtilizationThresholds, out *api.NodeResourceUtilizationThresholds, s conversion.Scope) error {
	out.Thresholds = *(*api.ResourceThresholds)(unsafe.Pointer(&in.Thresholds))
	out.TargetThresholds = *(*api.ResourceThresholds)(unsafe.Pointer(&in.TargetThresholds))
	out.NumberOfNodes = in.NumberOfNodes
	return nil
}

// Convert_v1alpha1_NodeResourceUtilizationThresholds_To_api_NodeResourceUtilizationThresholds is an autogenerated conversion function.
func Convert_v1alpha1_NodeResourceUtilizationThresholds_To_api_NodeResourceUtilizationThresholds(in *NodeResourceUtilizationThresholds, out *api.NodeResourceUtilizationThresholds, s conversion.Scope) error {
	return autoConvert_v1alpha1_NodeResourceUtilizationThresholds_To_api_NodeResourceUtilizationThresholds(in, out, s)
}

func autoConvert_api_NodeResourceUtilizationThresholds_To_v1alpha1_NodeResourceUtilizationThresholds(in *api.NodeResourceUtilizationThresholds, out *NodeResourceUtilizationThresholds, s conversion.Scope) error {
	out.Thresholds = *(*ResourceThresholds)(unsafe.Pointer(&in.Thresholds))
	out.TargetThresholds = *(*ResourceThresholds)(unsafe.Pointer(&in.TargetThresholds))
	out.NumberOfNodes = in.NumberOfNodes
	return nil
}

// Convert_api_NodeResourceUtilizationThresholds_To_v1alpha1_NodeResourceUtilizationThresholds is an autogenerated conversion function.
func Convert_api_NodeResourceUtilizationThresholds_To_v1alpha1_NodeResourceUtilizationThresholds(in *api.NodeResourceUtilizationThresholds, out *NodeResourceUtilizationThresholds, s conversion.Scope) error {
	return autoConvert_api_NodeResourceUtilizationThresholds_To_v1alpha1_NodeResourceUtilizationThresholds(in, out, s)
}

func autoConvert_v1alpha1_PodLifeTime_To_api_PodLifeTime(in *PodLifeTime, out *api.PodLifeTime, s conversion.Scope) error {
	out.MaxPodLifeTimeSeconds = (*uint)(unsafe.Pointer(in.MaxPodLifeTimeSeconds))
	out.PodStatusPhases = *(*[]string)(unsafe.Pointer(&in.PodStatusPhases))
	return nil
}

// Convert_v1alpha1_PodLifeTime_To_api_PodLifeTime is an autogenerated conversion function.
func Convert_v1alpha1_PodLifeTime_To_api_PodLifeTime(in *PodLifeTime, out *api.PodLifeTime, s conversion.Scope) error {
	return autoConvert_v1alpha1_PodLifeTime_To_api_PodLifeTime(in, out, s)
}

func autoConvert_api_PodLifeTime_To_v1alpha1_PodLifeTime(in *api.PodLifeTime, out *PodLifeTime, s conversion.Scope) error {
	out.MaxPodLifeTimeSeconds = (*uint)(unsafe.Pointer(in.MaxPodLifeTimeSeconds))
	out.PodStatusPhases = *(*[]string)(unsafe.Pointer(&in.PodStatusPhases))
	return nil
}

// Convert_api_PodLifeTime_To_v1alpha1_PodLifeTime is an autogenerated conversion function.
func Convert_api_PodLifeTime_To_v1alpha1_PodLifeTime(in *api.PodLifeTime, out *PodLifeTime, s conversion.Scope) error {
	return autoConvert_api_PodLifeTime_To_v1alpha1_PodLifeTime(in, out, s)
}

func autoConvert_v1alpha1_PodsHavingTooManyRestarts_To_api_PodsHavingTooManyRestarts(in *PodsHavingTooManyRestarts, out *api.PodsHavingTooManyRestarts, s conversion.Scope) error {
	out.PodRestartThreshold = in.PodRestartThreshold
	out.IncludingInitContainers = in.IncludingInitContainers
	return nil
}

// Convert_v1alpha1_PodsHavingTooManyRestarts_To_api_PodsHavingTooManyRestarts is an autogenerated conversion function.
func Convert_v1alpha1_PodsHavingTooManyRestarts_To_api_PodsHavingTooManyRestarts(in *PodsHavingTooManyRestarts, out *api.PodsHavingTooManyRestarts, s conversion.Scope) error {
	return autoConvert_v1alpha1_PodsHavingTooManyRestarts_To_api_PodsHavingTooManyRestarts(in, out, s)
}

func autoConvert_api_PodsHavingTooManyRestarts_To_v1alpha1_PodsHavingTooManyRestarts(in *api.PodsHavingTooManyRestarts, out *PodsHavingTooManyRestarts, s conversion.Scope) error {
	out.PodRestartThreshold = in.PodRestartThreshold
	out.IncludingInitContainers = in.IncludingInitContainers
	return nil
}

// Convert_api_PodsHavingTooManyRestarts_To_v1alpha1_PodsHavingTooManyRestarts is an autogenerated conversion function.
func Convert_api_PodsHavingTooManyRestarts_To_v1alpha1_PodsHavingTooManyRestarts(in *api.PodsHavingTooManyRestarts, out *PodsHavingTooManyRestarts, s conversion.Scope) error {
	return autoConvert_api_PodsHavingTooManyRestarts_To_v1alpha1_PodsHavingTooManyRestarts(in, out, s)
}

func autoConvert_v1alpha1_RemoveDuplicates_To_api_RemoveDuplicates(in *RemoveDuplicates, out *api.RemoveDuplicates, s conversion.Scope) error {
	out.ExcludeOwnerKinds = *(*[]string)(unsafe.Pointer(&in.ExcludeOwnerKinds))
	return nil
}

// Convert_v1alpha1_RemoveDuplicates_To_api_RemoveDuplicates is an autogenerated conversion function.
func Convert_v1alpha1_RemoveDuplicates_To_api_RemoveDuplicates(in *RemoveDuplicates, out *api.RemoveDuplicates, s conversion.Scope) error {
	return autoConvert_v1alpha1_RemoveDuplicates_To_api_RemoveDuplicates(in, out, s)
}

func autoConvert_api_RemoveDuplicates_To_v1alpha1_RemoveDuplicates(in *api.RemoveDuplicates, out *RemoveDuplicates, s conversion.Scope) error {
	out.ExcludeOwnerKinds = *(*[]string)(unsafe.Pointer(&in.ExcludeOwnerKinds))
	return nil
}

// Convert_api_RemoveDuplicates_To_v1alpha1_RemoveDuplicates is an autogenerated conversion function.
func Convert_api_RemoveDuplicates_To_v1alpha1_RemoveDuplicates(in *api.RemoveDuplicates, out *RemoveDuplicates, s conversion.Scope) error {
	return autoConvert_api_RemoveDuplicates_To_v1alpha1_RemoveDuplicates(in, out, s)
}

func autoConvert_v1alpha1_StrategyParameters_To_api_StrategyParameters(in *StrategyParameters, out *api.StrategyParameters, s conversion.Scope) error {
	out.NodeResourceUtilizationThresholds = (*api.NodeResourceUtilizationThresholds)(unsafe.Pointer(in.NodeResourceUtilizationThresholds))
	out.NodeAffinityType = *(*[]string)(unsafe.Pointer(&in.NodeAffinityType))
	out.PodsHavingTooManyRestarts = (*api.PodsHavingTooManyRestarts)(unsafe.Pointer(in.PodsHavingTooManyRestarts))
	out.PodLifeTime = (*api.PodLifeTime)(unsafe.Pointer(in.PodLifeTime))
	out.RemoveDuplicates = (*api.RemoveDuplicates)(unsafe.Pointer(in.RemoveDuplicates))
	out.FailedPods = (*api.FailedPods)(unsafe.Pointer(in.FailedPods))
	out.IncludeSoftConstraints = in.IncludeSoftConstraints
	out.Namespaces = (*api.Namespaces)(unsafe.Pointer(in.Namespaces))
	out.ThresholdPriority = (*int32)(unsafe.Pointer(in.ThresholdPriority))
	out.ThresholdPriorityClassName = in.ThresholdPriorityClassName
	out.LabelSelector = (*v1.LabelSelector)(unsafe.Pointer(in.LabelSelector))
	out.NodeFit = in.NodeFit
	out.Iterations = (*int32)(unsafe.Pointer(in.Iterations))
	return nil
}

// Convert_v1alpha1_StrategyParameters_To_api_StrategyParameters is an autogenerated conversion function.
func Convert_v1alpha1_StrategyParameters_To_api_StrategyParameters(in *StrategyParameters, out *api.StrategyParameters, s conversion.Scope) error {
	return autoConvert_v1alpha1_StrategyParameters_To_api_StrategyParameters(in, out, s)
}

func autoConvert_api_StrategyParameters_To_v1alpha1_StrategyParameters(in *api.StrategyParameters, out *StrategyParameters, s conversion.Scope) error {
	out.NodeResourceUtilizationThresholds = (*NodeResourceUtilizationThresholds)(unsafe.Pointer(in.NodeResourceUtilizationThresholds))
	out.NodeAffinityType = *(*[]string)(unsafe.Pointer(&in.NodeAffinityType))
	out.PodsHavingTooManyRestarts = (*PodsHavingTooManyRestarts)(unsafe.Pointer(in.PodsHavingTooManyRestarts))
	out.PodLifeTime = (*PodLifeTime)(unsafe.Pointer(in.PodLifeTime))
	out.RemoveDuplicates = (*RemoveDuplicates)(unsafe.Pointer(in.RemoveDuplicates))
	out.FailedPods = (*FailedPods)(unsafe.Pointer(in.FailedPods))
	out.IncludeSoftConstraints = in.IncludeSoftConstraints
	out.Namespaces = (*Namespaces)(unsafe.Pointer(in.Namespaces))
	out.ThresholdPriority = (*int32)(unsafe.Pointer(in.ThresholdPriority))
	out.ThresholdPriorityClassName = in.ThresholdPriorityClassName
	out.LabelSelector = (*v1.LabelSelector)(unsafe.Pointer(in.LabelSelector))
	out.NodeFit = in.NodeFit
	out.Iterations = (*int32)(unsafe.Pointer(in.Iterations))
	return nil
}

// Convert_api_StrategyParameters_To_v1alpha1_StrategyParameters is an autogenerated conversion function.
func Convert_api_StrategyParameters_To_v1alpha1_StrategyParameters(in *api.StrategyParameters, out *StrategyParameters, s conversion.Scope) error {
	return autoConvert_api_StrategyParameters_To_v1alpha1_StrategyParameters(in, out, s)
}