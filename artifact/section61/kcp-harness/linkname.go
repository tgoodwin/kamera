package main

import (
	"context"
	"unsafe"

	apibindingdeletion "github.com/kcp-dev/kcp/pkg/reconciler/apis/apibindingdeletion"
	corelogicalcluster "github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster"
	defaultapibinding "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/defaultapibindinglifecycle"
	initialization "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/initialization"
)

// Phase 1: exported types — reference directly
//
//go:linkname logicalClusterProcess github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster.(*Controller).process
func logicalClusterProcess(c *corelogicalcluster.Controller, ctx context.Context, key string) (bool, error)

//go:linkname apiBinderProcess github.com/kcp-dev/kcp/pkg/reconciler/tenancy/initialization.(*APIBinder).process
func apiBinderProcess(c *initialization.APIBinder, ctx context.Context, key string) error

//go:linkname defaultAPIBindingProcess github.com/kcp-dev/kcp/pkg/reconciler/tenancy/defaultapibindinglifecycle.(*DefaultAPIBindingController).process
func defaultAPIBindingProcess(c *defaultapibinding.DefaultAPIBindingController, ctx context.Context, key string) error

// Region 4: deletion controllers
//
//go:linkname apiBindingDeletionProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apibindingdeletion.(*Controller).process
func apiBindingDeletionProcess(c *apibindingdeletion.Controller, ctx context.Context, key string) error

// Region 2/3 bridge: apiexport controller (unexported type)
//
//go:linkname apiExportProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexport.(*controller).process
func apiExportProcess(c unsafe.Pointer, ctx context.Context, key string) error

// Region 4: logicalclustercleanup (unexported type)
//
//go:linkname logicalClusterCleanupProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/logicalclustercleanup.(*controller).process
func logicalClusterCleanupProcess(c unsafe.Pointer, ctx context.Context, key string) error

// Region 2: API binding lifecycle (unexported type)
//
//go:linkname apiBindingReconcilerProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding.(*controller).process
func apiBindingReconcilerProcess(c unsafe.Pointer, ctx context.Context, key string) (bool, error)

// Phase 2: unexported types — use unsafe.Pointer
//
//go:linkname extraAnnotationSyncProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/extraannotationsync.(*controller).process
func extraAnnotationSyncProcess(c unsafe.Pointer, ctx context.Context, key string) error

//go:linkname apiExportEndpointSliceProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointslice.(*controller).process
func apiExportEndpointSliceProcess(c unsafe.Pointer, ctx context.Context, key string) error

//go:linkname apiExportEndpointSliceURLsProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointsliceurls.(*controller).process
func apiExportEndpointSliceURLsProcess(c unsafe.Pointer, ctx context.Context, key string) (bool, error)
