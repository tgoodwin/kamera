package replay

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Operation Preconditions

// Delete Preconditions:

// DeleteOptions.Preconditions.UID: Object is only deleted if current UID matches
// DeleteOptions.Preconditions.ResourceVersion: Object is only deleted if current ResourceVersion matches

// Update/Patch Preconditions:

// ResourceVersion: For optimistic concurrency control
// Strategic Merge Patch includes the current state in its calculation
// Server-side Apply uses "field manager" ownership for conflict resolution

// Create Preconditions:

// No direct UID preconditions (as object doesn't exist yet)
// DryRun flag can be used to validate without committing

// Get Preconditions:

// No preconditions, but can filter responses

// PreconditionInfo stores preconditions extracted from client operation options
type PreconditionInfo struct {
	ResourceVersion *string    // For optimistic concurrency control
	UID             *types.UID // For delete operations with UID check
	DryRun          bool       // For operations that shouldn't affect state
}

// ExtractCreatePreconditions extracts preconditions from CreateOptions
func ExtractCreatePreconditions(opts []client.CreateOption) PreconditionInfo {
	createOpts := &client.CreateOptions{}
	for _, opt := range opts {
		opt.ApplyToCreate(createOpts)
	}

	return PreconditionInfo{
		DryRun: containsDryRun(createOpts.DryRun),
	}
}

// ExtractUpdatePreconditions extracts preconditions from UpdateOptions
func ExtractUpdatePreconditions(opts []client.UpdateOption) PreconditionInfo {
	updateOpts := &client.UpdateOptions{}
	for _, opt := range opts {
		opt.ApplyToUpdate(updateOpts)
	}

	return PreconditionInfo{
		DryRun: containsDryRun(updateOpts.DryRun),
	}
}

// ExtractDeletePreconditions extracts preconditions from DeleteOptions
func ExtractDeletePreconditions(opts []client.DeleteOption) PreconditionInfo {
	deleteOpts := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(deleteOpts)
	}

	var uid *types.UID
	if deleteOpts.Preconditions != nil && deleteOpts.Preconditions.UID != nil {
		uid = deleteOpts.Preconditions.UID
	}

	var resourceVersion *string
	if deleteOpts.Preconditions != nil && deleteOpts.Preconditions.ResourceVersion != nil {
		resourceVersion = deleteOpts.Preconditions.ResourceVersion
	}

	return PreconditionInfo{
		UID:             uid,
		ResourceVersion: resourceVersion,
		DryRun:          containsDryRun(deleteOpts.DryRun),
	}
}

// ExtractPatchPreconditions extracts preconditions from PatchOptions
func ExtractPatchPreconditions(opts []client.PatchOption) PreconditionInfo {
	patchOpts := &client.PatchOptions{}
	for _, opt := range opts {
		opt.ApplyToPatch(patchOpts)
	}

	// Patches can specify Force conflict resolution
	return PreconditionInfo{
		DryRun: containsDryRun(patchOpts.DryRun),
	}
}

func ExtractDeleteAllOfPreconditions(opts []client.DeleteAllOfOption) PreconditionInfo {
	deleteAllOfOpts := &client.DeleteAllOfOptions{}
	for _, opt := range opts {
		opt.ApplyToDeleteAllOf(deleteAllOfOpts)
	}

	// DeleteAllOf doesn't support direct UID preconditions like individual Delete
	// But it does support resource version and dry run
	var resourceVersion *string
	if deleteAllOfOpts.Preconditions != nil && deleteAllOfOpts.Preconditions.ResourceVersion != nil {
		resourceVersion = deleteAllOfOpts.Preconditions.ResourceVersion
	}

	return PreconditionInfo{
		ResourceVersion: resourceVersion,
		DryRun:          containsDryRun(deleteAllOfOpts.DryRun),
	}
}

// ExtractStatusUpdatePreconditions extracts preconditions from SubResourceUpdateOptions
func ExtractStatusUpdatePreconditions(opts []client.SubResourceUpdateOption) PreconditionInfo {
	updateOpts := &client.SubResourceUpdateOptions{}
	for _, opt := range opts {
		opt.ApplyToSubResourceUpdate(updateOpts)
	}

	return PreconditionInfo{
		DryRun: containsDryRun(updateOpts.DryRun),
	}
}

// ExtractStatusPatchPreconditions extracts preconditions from SubResourcePatchOptions
func ExtractStatusPatchPreconditions(opts []client.SubResourcePatchOption) PreconditionInfo {
	patchOpts := &client.SubResourcePatchOptions{}
	for _, opt := range opts {
		opt.ApplyToSubResourcePatch(patchOpts)
	}

	return PreconditionInfo{
		DryRun: containsDryRun(patchOpts.DryRun),
	}
}

// ExtractSubResourceCreatePreconditions extracts preconditions from SubResourceCreateOptions
func ExtractSubResourceCreatePreconditions(opts []client.SubResourceCreateOption) PreconditionInfo {
	createOpts := &client.SubResourceCreateOptions{}
	for _, opt := range opts {
		opt.ApplyToSubResourceCreate(createOpts)
	}

	return PreconditionInfo{
		DryRun: containsDryRun(createOpts.DryRun),
	}
}

// Helper to check for DryRun in options
func containsDryRun(dryRun []string) bool {
	for _, mode := range dryRun {
		if mode == metav1.DryRunAll {
			return true
		}
	}
	return false
}
