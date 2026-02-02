package testcommon

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func MustBeIdentical(t *testing.T, expected, got any, opts ...cmp.Option) {
	t.Helper()

	if diff := cmp.Diff(expected, got, append([]cmp.Option{cmpOptionJSONTransformer()}, opts...)...); diff != "" {
		require.FailNow(t, "(-want,+got)", diff)
	}
}

func cmpOptionJSONTransformer() cmp.Option {
	return cmp.FilterValues(
		func(x, y []byte) bool { return json.Valid(x) && json.Valid(y) },
		cmp.Transformer("ParseJSON", func(in []byte) (out interface{}) {
			if err := json.Unmarshal(in, &out); err != nil {
				panic(err) // should never occur given previous filter to ensure valid JSON
			}
			return out
		}))
}
