package replay

import (
	"reflect"
	"time"

	"github.com/tgoodwin/kamera/pkg/simclock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var metav1TimeType = reflect.TypeOf(metav1.Time{})

// normalizeSimulatedConditionTimes keeps controller-written status timestamps
// in the same clock domain as replay-created object timestamps. Dependencies
// that call metav1.Now directly can otherwise write wall-clock values.
func normalizeSimulatedConditionTimes(obj client.Object) {
	normalizeSimulatedConditionValue(reflect.ValueOf(obj), simclock.Now())
}

func normalizeSimulatedConditionValue(value reflect.Value, now time.Time) {
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		normalizeSimulatedConditionValue(value.Elem(), now)
		return
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if value.Type().Field(i).Name == "LastTransitionTime" && field.Type() == metav1TimeType && field.CanSet() {
				transition := field.Interface().(metav1.Time).Time
				if transition.After(now) {
					field.Set(reflect.ValueOf(metav1.NewTime(now)))
				}
				continue
			}
			normalizeSimulatedConditionValue(field, now)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			normalizeSimulatedConditionValue(value.Index(i), now)
		}
	}
}
