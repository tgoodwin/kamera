package main

import (
	"context"
	"fmt"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
)

// deterministicCloudProvider wraps the fake CloudProvider to assign
// deterministic ProviderIDs based on the NodeClaim name, instead of
// using test.RandomProviderID() which is seeded with time.Now().UnixNano().
// This ensures reproducible node names across runs with the same seed.
type deterministicCloudProvider struct {
	*fake.CloudProvider
}

// Reset clears accumulated cloud provider state (created/deleted NodeClaims)
// so each exploration trial starts fresh.
func (d *deterministicCloudProvider) Reset() {
	d.CreatedNodeClaims = map[string]*v1.NodeClaim{}
	d.CreateCalls = nil
	d.DeleteCalls = nil
	d.GetCalls = nil
}

func (d *deterministicCloudProvider) Create(ctx context.Context, nodeClaim *v1.NodeClaim) (*v1.NodeClaim, error) {
	created, err := d.CloudProvider.Create(ctx, nodeClaim)
	if err != nil {
		return nil, err
	}

	// Replace the nondeterministic ProviderID with one derived from the
	// NodeClaim name, which is deterministic (assigned by nameGeneratingClient).
	oldID := created.Status.ProviderID
	newID := fmt.Sprintf("fake:///%s", nodeClaim.Name)
	created.Status.ProviderID = newID

	// Fix the internal tracking map so Get/Delete/List remain consistent.
	delete(d.CreatedNodeClaims, oldID)
	d.CreatedNodeClaims[newID] = created

	return created, nil
}
