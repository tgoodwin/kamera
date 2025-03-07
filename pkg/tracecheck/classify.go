package tracecheck

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type StateClassifier struct {
	// The resolver is needed to get the actual object from a version hash
	resolver VersionManager
}

func NewStateClassifier(resolver VersionManager) *StateClassifier {
	return &StateClassifier{
		resolver: resolver,
	}
}

func (s *StateClassifier) ClassifyResults(states []ConvergedState, predicate StatePredicate) []ClassifiedState {
	classified := []ClassifiedState{}
	for i, state := range states {
		classifiedState := ClassifiedState{
			ID:             fmt.Sprintf("state-%02d", i),
			State:          state,
			PassedChecks:   []string{},
			FailureReasons: []string{},
		}

		// Evaluate each predicate
		// for name, predicate := range predicates {
		passed, reason := predicate(state.State)
		if passed {
			classifiedState.Classification = "happy"
		} else {
			classifiedState.Classification = "bad"
			classifiedState.FailureReasons = append(classifiedState.FailureReasons, reason)
		}
		// if passed {
		// 	classifiedState.PassedChecks = append(classifiedState.PassedChecks)
		// } else {
		// 	classifiedState.FailureReasons = append(classifiedState.FailureReasons,
		// 		fmt.Sprintf("%s: %s", name, reason))
		// }
		// }

		// Classify the state
		// if len(classifiedState.FailureReasons) == 0 {
		// 	classifiedState.Classification = "happy"
		// } else {
		// 	classifiedState.Classification = "bad"
		// }
		classified = append(classified, classifiedState)
	}

	return classified
}

func (s *StateClassifier) NewPredicateBuilder() *PredicateBuilder {
	return NewPredicateBuilder(s.resolver)
}

// StatePredicate represents a function that evaluates whether a state meets a specific criterion
type StatePredicate func(state StateNode) (bool, string)

// ClassifiedResult organizes converged states by classification
type ClassifiedResult struct {
	// States that satisfy all predicates
	HappyPaths []ClassifiedState

	// States that fail at least one predicate
	BadPaths []ClassifiedState

	// Original result
	OriginalResult *Result
}

// ClassifiedState holds a state with its classification information
type ClassifiedState struct {
	State          ConvergedState
	ID             string   // something we produce to facilitate bookkeeping during analysis
	Classification string   // "happy" or "bad"
	FailureReasons []string // Empty for happy paths
	PassedChecks   []string // Descriptions of passed checks
}

// PredicateBuilder helps create complex state predicates
type PredicateBuilder struct {
	// The resolver is needed to get the actual object from a version hash
	resolver VersionManager
}

// NewPredicateBuilder creates a new predicate builder with the given resolver
func NewPredicateBuilder(resolver VersionManager) *PredicateBuilder {
	return &PredicateBuilder{
		resolver: resolver,
	}
}

// ObjectExists creates a predicate that checks if an object exists
func (b *PredicateBuilder) ObjectExists(kind, objectID string) StatePredicate {
	return func(state StateNode) (bool, string) {
		key := snapshot.IdentityKey{Kind: kind, ObjectID: objectID}
		_, exists := state.Objects()[key]
		if !exists {
			return false, fmt.Sprintf("Object %s/%s does not exist", kind, objectID)
		}
		return true, ""
	}
}

// ObjectsCountOfKind creates a predicate that checks the count of objects of a given kind
func (b *PredicateBuilder) ObjectsCountOfKind(kind string, expectedCount int) StatePredicate {
	return func(state StateNode) (bool, string) {
		count := 0
		for key := range state.Objects() {
			if key.Kind == kind {
				count++
			}
		}

		if count != expectedCount {
			return false, fmt.Sprintf("Found %d objects of kind %s, expected %d", count, kind, expectedCount)
		}
		return true, ""
	}
}

// ObjectField creates a predicate that checks a specific field in an object
func (b *PredicateBuilder) ObjectField(kind, objectID string, fieldPath []string, expectedValue interface{}) StatePredicate {
	return func(state StateNode) (bool, string) {
		key := snapshot.IdentityKey{Kind: kind, ObjectID: objectID}
		versionHash, exists := state.Objects()[key]
		if !exists {
			return false, fmt.Sprintf("Object %s/%s does not exist", kind, objectID)
		}

		// Resolve the object
		obj := b.resolver.Resolve(versionHash)
		if obj == nil {
			return false, fmt.Sprintf("Failed to resolve object %s/%s", kind, objectID)
		}

		// Navigate and check field
		value, found, err := unstructured.NestedFieldNoCopy(obj.Object, fieldPath...)
		if err != nil || !found {
			return false, fmt.Sprintf("Field %s not found in %s/%s", strings.Join(fieldPath, "."), kind, objectID)
		}

		if !reflect.DeepEqual(value, expectedValue) {
			return false, fmt.Sprintf("Field %s in %s/%s is %v, expected %v",
				strings.Join(fieldPath, "."), kind, objectID, value, expectedValue)
		}

		return true, ""
	}
}

// Custom creates a predicate with custom logic
func (b *PredicateBuilder) Custom(description string, evaluate func(state StateNode) bool) StatePredicate {
	return func(state StateNode) (bool, string) {
		if !evaluate(state) {
			return false, description
		}
		return true, ""
	}
}

// And combines multiple predicates with AND logic - all must pass
func (b *PredicateBuilder) And(predicates ...StatePredicate) StatePredicate {
	return func(state StateNode) (bool, string) {
		for _, predicate := range predicates {
			passed, reason := predicate(state)
			if !passed {
				return false, reason
			}
		}
		return true, ""
	}
}

// Or combines multiple predicates with OR logic - at least one must pass
func (b *PredicateBuilder) Or(predicates ...StatePredicate) StatePredicate {
	return func(state StateNode) (bool, string) {
		var reasons []string
		for _, predicate := range predicates {
			passed, reason := predicate(state)
			if passed {
				return true, ""
			}
			reasons = append(reasons, reason)
		}
		return false, fmt.Sprintf("None of the conditions passed: %s", strings.Join(reasons, "; "))
	}
}

// Not inverts a predicate
func (b *PredicateBuilder) Not(predicate StatePredicate) StatePredicate {
	return func(state StateNode) (bool, string) {
		passed, reason := predicate(state)
		if passed {
			return false, fmt.Sprintf("Condition unexpectedly passed: %s", reason)
		}
		return true, ""
	}
}

// ClassifyResult evaluates converged states against user-defined predicates
func ClassifyResult(result *Result, predicates map[string]StatePredicate) *ClassifiedResult {
	classified := &ClassifiedResult{
		HappyPaths:     []ClassifiedState{},
		BadPaths:       []ClassifiedState{},
		OriginalResult: result,
	}

	for _, state := range result.ConvergedStates {
		classifiedState := ClassifiedState{
			State:        state,
			PassedChecks: []string{},
		}

		// Evaluate each predicate
		for name, predicate := range predicates {
			passed, reason := predicate(state.State)
			if passed {
				classifiedState.PassedChecks = append(classifiedState.PassedChecks, name)
			} else {
				classifiedState.FailureReasons = append(classifiedState.FailureReasons,
					fmt.Sprintf("%s: %s", name, reason))
			}
		}

		// Classify the state
		if len(classifiedState.FailureReasons) == 0 {
			classifiedState.Classification = "happy"
			classified.HappyPaths = append(classified.HappyPaths, classifiedState)
		} else {
			classifiedState.Classification = "bad"
			classified.BadPaths = append(classified.BadPaths, classifiedState)
		}
	}

	return classified
}

// SummarizeClassification prints a summary of the classification result
func SummarizeClassification(classified *ClassifiedResult) {
	fmt.Printf("Classification summary:\n")
	fmt.Printf("  Happy paths: %d\n", len(classified.HappyPaths))
	fmt.Printf("  Bad paths: %d\n", len(classified.BadPaths))

	if len(classified.BadPaths) > 0 {
		fmt.Printf("\nBad path details:\n")
		for i, badPath := range classified.BadPaths {
			fmt.Printf("Bad path #%d: %s\n", i+1, badPath.State.ID)
			fmt.Printf("  Failure reasons:\n")
			for _, reason := range badPath.FailureReasons {
				fmt.Printf("    - %s\n", reason)
			}
			if len(badPath.PassedChecks) > 0 {
				fmt.Printf("  Passed checks:\n")
				for _, check := range badPath.PassedChecks {
					fmt.Printf("    - %s\n", check)
				}
			}
			fmt.Println()
		}
	}
}
