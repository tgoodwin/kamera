package main

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/metadata"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpmetadata "github.com/kcp-dev/client-go/metadata"
)

// stubMetadataClient implements kcpmetadata.ClusterInterface with no-op operations.
// Used by apibindingdeletion which needs a metadata client to list/delete bound CRs.
// In our harness scenarios there are no bound CRs, so all operations return empty results.
type stubMetadataClient struct{}

var _ kcpmetadata.ClusterInterface = &stubMetadataClient{}

func (s *stubMetadataClient) Cluster(_ logicalcluster.Path) metadata.Interface {
	return &stubMetadataInterface{}
}

func (s *stubMetadataClient) Resource(_ schema.GroupVersionResource) kcpmetadata.ResourceClusterInterface {
	return &stubResourceClusterInterface{}
}

type stubMetadataInterface struct{}

func (s *stubMetadataInterface) Resource(_ schema.GroupVersionResource) metadata.Getter {
	return &stubMetadataGetter{}
}

type stubResourceClusterInterface struct{}

func (s *stubResourceClusterInterface) Cluster(_ logicalcluster.Path) metadata.Getter {
	return &stubMetadataGetter{}
}

func (s *stubResourceClusterInterface) List(_ context.Context, _ metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	return &metav1.PartialObjectMetadataList{}, nil
}

func (s *stubResourceClusterInterface) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

// stubMetadataGetter implements metadata.Getter (which embeds ResourceInterface + Namespace).
type stubMetadataGetter struct{}

func (s *stubMetadataGetter) Namespace(_ string) metadata.ResourceInterface {
	return s
}

func (s *stubMetadataGetter) Delete(_ context.Context, _ string, _ metav1.DeleteOptions, _ ...string) error {
	return nil
}

func (s *stubMetadataGetter) DeleteCollection(_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions) error {
	return nil
}

func (s *stubMetadataGetter) Create(_ context.Context, _ *metav1.PartialObjectMetadata, _ metav1.CreateOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	return nil, nil
}

func (s *stubMetadataGetter) Update(_ context.Context, _ *metav1.PartialObjectMetadata, _ metav1.UpdateOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	return nil, nil
}

func (s *stubMetadataGetter) UpdateStatus(_ context.Context, _ *metav1.PartialObjectMetadata, _ metav1.UpdateOptions) (*metav1.PartialObjectMetadata, error) {
	return nil, nil
}

func (s *stubMetadataGetter) Get(_ context.Context, _ string, _ metav1.GetOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	return nil, nil
}

func (s *stubMetadataGetter) List(_ context.Context, _ metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	return &metav1.PartialObjectMetadataList{}, nil
}

func (s *stubMetadataGetter) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (s *stubMetadataGetter) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	return nil, nil
}
