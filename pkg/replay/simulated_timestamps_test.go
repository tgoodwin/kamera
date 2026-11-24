package replay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/simclock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNormalizeSimulatedConditionTimes(t *testing.T) {
	simclock.Configure(time.Unix(0, 0), time.Second)
	restore := simclock.SetDepth(5)
	defer restore()

	node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
		{
			Type:               corev1.NodeReady,
			LastTransitionTime: metav1.NewTime(time.Date(2026, 11, 23, 23, 45, 54, 0, time.UTC)),
		},
		{
			Type:               corev1.NodeMemoryPressure,
			LastTransitionTime: metav1.NewTime(time.Unix(1, 0)),
		},
	}}}

	normalizeSimulatedConditionTimes(node)

	require.Equal(t, time.Unix(5, 0), node.Status.Conditions[0].LastTransitionTime.Time)
	require.Equal(t, time.Unix(1, 0), node.Status.Conditions[1].LastTransitionTime.Time)
}
