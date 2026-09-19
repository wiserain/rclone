package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSizeInfoUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantSize   int64
		wantFormat string
		wantErr    bool
	}{
		{name: "number", body: `{"size":123,"size_format":"123 B"}`, wantSize: 123, wantFormat: "123 B"},
		{name: "string", body: `{"size":"123"}`, wantSize: 123},
		{name: "decimal", body: `{"size":"123.75"}`, wantSize: 123},
		{name: "exponent", body: `{"size":"1.25e3"}`, wantSize: 1250},
		{name: "null", body: `{"size":null}`},
		{name: "empty", body: `{"size":""}`},
		{name: "infinite", body: `{"size":"Inf"}`, wantErr: true},
		{name: "out of range", body: `{"size":"9223372036854775808"}`, wantErr: true},
		{name: "invalid", body: `{"size":"not-a-size"}`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var info SizeInfo
			err := json.Unmarshal([]byte(test.body), &info)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantSize, info.Size)
			assert.Equal(t, test.wantFormat, info.SizeFormat)
		})
	}
}
