package snapshot

import "github.com/tgoodwin/kamera/pkg/util"

// CanonicalGroupKind returns the canonical group/kind combination for the identity key.
func (ik IdentityKey) CanonicalGroupKind() string {
	return util.CanonicalGroupKind(ik.Group, ik.Kind)
}

// CanonicalGroupKind returns the canonical group/kind combination for the resource key.
func (rk ResourceKey) CanonicalGroupKind() string {
	return util.CanonicalGroupKind(rk.Group, rk.Kind)
}

// CanonicalGroupKind returns the canonical group/kind combination for the composite key.
func (ck CompositeKey) CanonicalGroupKind() string {
	return ck.IdentityKey.CanonicalGroupKind()
}
