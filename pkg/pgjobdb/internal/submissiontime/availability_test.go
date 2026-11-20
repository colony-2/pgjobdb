package submissiontime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEffective(t *testing.T) {
	v, err := Effective(nil)
	require.NoError(t, err)
	require.Nil(t, v)
	for _, input := range []string{
		"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999Z",
		"1900-01-01T00:00:00.000001Z", "1969-12-31T23:59:59.999999001Z",
		"1970-01-01T00:00:00Z", "1970-01-01T00:00:00.000000001Z",
		"2300-01-01T00:00:00.123456789Z", "2026-11-16T23:59:59.999999999Z",
		"2026-12-31T23:59:59.999999999Z", "9999-12-31T23:59:59.999998999Z",
		"0001-01-01T01:00:00+01:00", "9999-12-31T22:59:59.999999-01:00",
	} {
		t.Run(input, func(t *testing.T) {
			original, err := time.Parse(time.RFC3339Nano, input)
			require.NoError(t, err)
			before := original
			got, err := Effective(&original)
			require.NoError(t, err)
			require.Equal(t, before, original)
			require.NotSame(t, &original, got)
			require.Same(t, time.UTC, got.Location())
			require.False(t, got.Before(original))
			require.Less(t, got.Sub(original), time.Microsecond)
			require.Zero(t, got.Nanosecond()%1000)
			if original.Nanosecond()%1000 == 0 {
				require.True(t, got.Equal(original))
			}
			offset := original.In(time.FixedZone("east", 14*3600))
			same, err := Effective(&offset)
			require.NoError(t, err)
			require.Equal(t, got, same)
		})
	}
	for _, at := range []time.Time{minimum.Add(-time.Nanosecond), maximum.Add(time.Nanosecond), maximum.Add(499 * time.Nanosecond), maximum.Add(999 * time.Nanosecond), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		got, err := Effective(&at)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestValidateDescriptor(t *testing.T) {
	for _, descriptor := range []string{
		"opaque native descriptor", `{"custom":"native"}`, `{"Kind":"restart"}`,
		`{"Kind":"submit","Job":{}}`, `{"Kind":"submit","Job":{"AvailableAt":null}}`,
		`{"Kind":"submit","Job":{"AvailableAt":"0001-01-01T00:00:00Z"}}`,
		`{"Kind":"submit","Job":{"AvailableAt":"2300-01-01T00:00:00.123456789Z"}}`,
	} {
		require.NoError(t, ValidateDescriptor(descriptor), descriptor)
	}
	for _, descriptor := range []string{
		`{"Kind":"submit","Job":{"AvailableAt":"0000-12-31T23:59:59.999999999Z"}}`,
		`{"Kind":"submit","Job":{"AvailableAt":"9999-12-31T23:59:59.999999001Z"}}`,
		`{"Kind":"submit","Job":{"AvailableAt":"bad"}}`,
		`{"Kind":"submit","Job":{"AvailableAt":12}}`,
	} {
		require.Error(t, ValidateDescriptor(descriptor), descriptor)
	}
}
