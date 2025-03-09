package replay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Mock client.CreateOption implementation
type mockCreateOption struct {
	dryRun []string
}

func (m mockCreateOption) ApplyToCreate(options *client.CreateOptions) {
	options.DryRun = m.dryRun
}

// Mock client.UpdateOption implementation
type mockUpdateOption struct {
	dryRun []string
}

func (m mockUpdateOption) ApplyToUpdate(options *client.UpdateOptions) {
	options.DryRun = m.dryRun
}

// Mock client.DeleteOption implementation
type mockDeleteOption struct {
	dryRun          []string
	uid             *types.UID
	resourceVersion *string
}

func (m mockDeleteOption) ApplyToDelete(options *client.DeleteOptions) {
	options.DryRun = m.dryRun
	if m.uid != nil || m.resourceVersion != nil {
		if options.Preconditions == nil {
			options.Preconditions = &metav1.Preconditions{}
		}
		options.Preconditions.UID = m.uid
		options.Preconditions.ResourceVersion = m.resourceVersion
	}
}

// Mock client.PatchOption implementation
type mockPatchOption struct {
	dryRun []string
}

func (m mockPatchOption) ApplyToPatch(options *client.PatchOptions) {
	options.DryRun = m.dryRun
}

// Mock client.DeleteAllOfOption implementation
type mockDeleteAllOfOption struct {
	dryRun          []string
	resourceVersion *string
}

func (m mockDeleteAllOfOption) ApplyToDeleteAllOf(options *client.DeleteAllOfOptions) {
	options.DryRun = m.dryRun
	if m.resourceVersion != nil {
		if options.Preconditions == nil {
			options.Preconditions = &metav1.Preconditions{}
		}
		options.Preconditions.ResourceVersion = m.resourceVersion
	}
}

// Mock client.SubResourceUpdateOption implementation
type mockSubResourceUpdateOption struct {
	dryRun []string
}

func (m mockSubResourceUpdateOption) ApplyToSubResourceUpdate(options *client.SubResourceUpdateOptions) {
	options.DryRun = m.dryRun
}

// Mock client.SubResourcePatchOption implementation
type mockSubResourcePatchOption struct {
	dryRun []string
}

func (m mockSubResourcePatchOption) ApplyToSubResourcePatch(options *client.SubResourcePatchOptions) {
	options.DryRun = m.dryRun
}

// Mock client.SubResourceCreateOption implementation
type mockSubResourceCreateOption struct {
	dryRun []string
}

func (m mockSubResourceCreateOption) ApplyToSubResourceCreate(options *client.SubResourceCreateOptions) {
	options.DryRun = m.dryRun
}

func TestExtractCreatePreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.CreateOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.CreateOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.CreateOption{
				mockCreateOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractCreatePreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestExtractUpdatePreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.UpdateOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.UpdateOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.UpdateOption{
				mockUpdateOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractUpdatePreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestExtractDeletePreconditions(t *testing.T) {
	uid := types.UID("test-uid")
	resourceVersion := "12345"

	tests := []struct {
		name     string
		opts     []client.DeleteOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.DeleteOption{},
			expected: PreconditionInfo{
				DryRun:          false,
				UID:             nil,
				ResourceVersion: nil,
			},
		},
		{
			name: "With DryRun",
			opts: []client.DeleteOption{
				mockDeleteOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun:          true,
				UID:             nil,
				ResourceVersion: nil,
			},
		},
		{
			name: "With UID precondition",
			opts: []client.DeleteOption{
				mockDeleteOption{uid: &uid},
			},
			expected: PreconditionInfo{
				DryRun:          false,
				UID:             &uid,
				ResourceVersion: nil,
			},
		},
		{
			name: "With ResourceVersion precondition",
			opts: []client.DeleteOption{
				mockDeleteOption{resourceVersion: &resourceVersion},
			},
			expected: PreconditionInfo{
				DryRun:          false,
				UID:             nil,
				ResourceVersion: &resourceVersion,
			},
		},
		{
			name: "With all preconditions",
			opts: []client.DeleteOption{
				mockDeleteOption{
					dryRun:          []string{metav1.DryRunAll},
					uid:             &uid,
					resourceVersion: &resourceVersion,
				},
			},
			expected: PreconditionInfo{
				DryRun:          true,
				UID:             &uid,
				ResourceVersion: &resourceVersion,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractDeletePreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)

			if tt.expected.UID == nil {
				assert.Nil(t, result.UID)
			} else {
				assert.Equal(t, *tt.expected.UID, *result.UID)
			}

			if tt.expected.ResourceVersion == nil {
				assert.Nil(t, result.ResourceVersion)
			} else {
				assert.Equal(t, *tt.expected.ResourceVersion, *result.ResourceVersion)
			}
		})
	}
}

func TestExtractPatchPreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.PatchOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.PatchOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.PatchOption{
				mockPatchOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractPatchPreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestExtractDeleteAllOfPreconditions(t *testing.T) {
	resourceVersion := "12345"

	tests := []struct {
		name     string
		opts     []client.DeleteAllOfOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.DeleteAllOfOption{},
			expected: PreconditionInfo{
				DryRun:          false,
				ResourceVersion: nil,
			},
		},
		{
			name: "With DryRun",
			opts: []client.DeleteAllOfOption{
				mockDeleteAllOfOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun:          true,
				ResourceVersion: nil,
			},
		},
		{
			name: "With ResourceVersion",
			opts: []client.DeleteAllOfOption{
				mockDeleteAllOfOption{resourceVersion: &resourceVersion},
			},
			expected: PreconditionInfo{
				DryRun:          false,
				ResourceVersion: &resourceVersion,
			},
		},
		{
			name: "With all options",
			opts: []client.DeleteAllOfOption{
				mockDeleteAllOfOption{
					dryRun:          []string{metav1.DryRunAll},
					resourceVersion: &resourceVersion,
				},
			},
			expected: PreconditionInfo{
				DryRun:          true,
				ResourceVersion: &resourceVersion,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractDeleteAllOfPreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)

			if tt.expected.ResourceVersion == nil {
				assert.Nil(t, result.ResourceVersion)
			} else {
				assert.Equal(t, *tt.expected.ResourceVersion, *result.ResourceVersion)
			}
		})
	}
}

func TestExtractStatusUpdatePreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.SubResourceUpdateOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.SubResourceUpdateOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.SubResourceUpdateOption{
				mockSubResourceUpdateOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractStatusUpdatePreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestExtractStatusPatchPreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.SubResourcePatchOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.SubResourcePatchOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.SubResourcePatchOption{
				mockSubResourcePatchOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractStatusPatchPreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestExtractSubResourceCreatePreconditions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []client.SubResourceCreateOption
		expected PreconditionInfo
	}{
		{
			name: "No options",
			opts: []client.SubResourceCreateOption{},
			expected: PreconditionInfo{
				DryRun: false,
			},
		},
		{
			name: "With DryRun",
			opts: []client.SubResourceCreateOption{
				mockSubResourceCreateOption{dryRun: []string{metav1.DryRunAll}},
			},
			expected: PreconditionInfo{
				DryRun: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractSubResourceCreatePreconditions(tt.opts)
			assert.Equal(t, tt.expected.DryRun, result.DryRun)
		})
	}
}

func TestContainsDryRun(t *testing.T) {
	tests := []struct {
		name     string
		dryRun   []string
		expected bool
	}{
		{
			name:     "nil slice",
			dryRun:   nil,
			expected: false,
		},
		{
			name:     "empty slice",
			dryRun:   []string{},
			expected: false,
		},
		{
			name:     "with DryRunAll",
			dryRun:   []string{metav1.DryRunAll},
			expected: true,
		},
		{
			name:     "with multiple values including DryRunAll",
			dryRun:   []string{"other", metav1.DryRunAll},
			expected: true,
		},
		{
			name:     "without DryRunAll",
			dryRun:   []string{"other"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsDryRun(tt.dryRun)
			assert.Equal(t, tt.expected, result)
		})
	}
}
