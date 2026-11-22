package integration_test

import (
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNativeGoalMembershipIsImmutable(t *testing.T) {
	rt := conformanceRuntimeFactory(t)(t)
	_, exists := reflect.TypeOf(rt).MethodByName("AddGoalCandidate")
	require.False(t, exists)
	// These public tests retain ordinary input/artifact and restart behavior while
	// requiring admission and protected terminal records from the native backend.
	runtimetest.RunGoalAdmissionConformance(t, rt)
	runtimetest.RunTerminalFailureConformance(t, rt)
}
