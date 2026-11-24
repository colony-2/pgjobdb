package pgjobdb_test

import (
	"math"
	"testing"
	"time"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffTimeBoundaries(t *testing.T) {
	for _, ns := range []int64{0, 1, 999, 1000, 1001, math.MaxInt64 - 1, math.MaxInt64} {
		wake := time.Unix(0, ns).UTC()
		index := pgjobdb.ResourceIndexTime(wake)
		require.False(t, index.Before(wake), "ns=%d", ns)
		require.Less(t, index.Sub(wake), time.Microsecond)
		require.Zero(t, index.Nanosecond()%1000)
	}
}
