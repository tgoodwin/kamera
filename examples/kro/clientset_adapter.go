package main

import (
	"context"
	"net/http"

	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	kroclient "github.com/kubernetes-sigs/kro/pkg/client"
	"github.com/tgoodwin/kamera/pkg/replay"
)

// replayClientSet implements kroclient.SetInterface backed by a kamera replay client.
type replayClientSet struct {
	dynamicClient dynamic.Interface
	replayClient  ctrlclient.Client
	restMapper    meta.RESTMapper
}

var _ kroclient.SetInterface = (*replayClientSet)(nil)

func newReplayClientSet(c ctrlclient.Client, mapper meta.RESTMapper) *replayClientSet {
	return &replayClientSet{
		dynamicClient: replay.NewDynamicClient(c, mapper),
		replayClient:  c,
		restMapper:    mapper,
	}
}

func (s *replayClientSet) Dynamic() dynamic.Interface                                { return s.dynamicClient }
func (s *replayClientSet) RESTMapper() meta.RESTMapper                               { return s.restMapper }
func (s *replayClientSet) SetRESTMapper(m meta.RESTMapper)                           { s.restMapper = m }
func (s *replayClientSet) HTTPClient() *http.Client                                  { return nil }
func (s *replayClientSet) RESTConfig() *rest.Config                                  { return nil }
func (s *replayClientSet) Kubernetes() kubernetes.Interface                          { return nil }
func (s *replayClientSet) APIExtensionsV1() apiextensionsv1.ApiextensionsV1Interface { return nil }
func (s *replayClientSet) WithImpersonation(user string) (kroclient.SetInterface, error) {
	return s, nil
}

func (s *replayClientSet) CRD(cfg kroclient.CRDWrapperConfig) kroclient.CRDInterface {
	return &replayCRDClient{inner: s.replayClient}
}

// replayCRDClient implements kroclient.CRDInterface using the replay client.
type replayCRDClient struct {
	inner ctrlclient.Client
}

var _ kroclient.CRDInterface = (*replayCRDClient)(nil)

func (c *replayCRDClient) Ensure(ctx context.Context, crd extv1.CustomResourceDefinition, _ bool) error {
	existing := &extv1.CustomResourceDefinition{}
	err := c.inner.Get(ctx, ctrlclient.ObjectKey{Name: crd.Name}, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return c.inner.Create(ctx, &crd)
		}
		return err
	}
	existing.Spec = crd.Spec
	existing.Labels = crd.Labels
	return c.inner.Update(ctx, existing)
}

func (c *replayCRDClient) Delete(ctx context.Context, name string) error {
	crd := &extv1.CustomResourceDefinition{}
	crd.SetName(name)
	return ctrlclient.IgnoreNotFound(c.inner.Delete(ctx, crd))
}

func (c *replayCRDClient) Get(ctx context.Context, name string) (*extv1.CustomResourceDefinition, error) {
	crd := &extv1.CustomResourceDefinition{}
	if err := c.inner.Get(ctx, ctrlclient.ObjectKey{Name: name}, crd); err != nil {
		return nil, err
	}
	return crd, nil
}
