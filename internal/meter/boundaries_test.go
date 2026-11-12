package meter

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	"gaigate/internal/provider"
	"github.com/stretchr/testify/require"
)

func TestInputAudioAndToolObjects(t *testing.T) {
	for _, tc := range []struct {
		message string
		want    int64
	}{
		// Golden canonical message-array lengths include all structure and
		// escaped tool argument quotes; these are character proxies only.
		{`{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"YWJj","format":"wav"}}]}`, 97},
		{`{"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{\"x\":1}"}}]}`, 129},
	} {
		r := request()
		r.Messages[0] = []byte(tc.message)
		before := append([]byte(nil), r.Messages[0]...)
		c, err := NewCollector(r, 1)
		require.NoError(t, err)
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, Measurement{Usage: provider.Usage{InputTokens: tc.want}, Estimated: true}, m)
		require.Equal(t, before, []byte(r.Messages[0]))
	}
}

func TestCollectorImplementationHasNoIOOrWorkerCapability(t *testing.T) {
	// Architectural mechanism check, paired with actual --network none tests:
	// implementation dependencies are pure standard-library algorithms plus
	// the shared provider types; no hidden client/SQL/process/worker capability.
	allowed := map[string]bool{"crypto/sha256": true, "encoding/binary": true, "errors": true, "hash": true, "math": true, "math/big": true, "strconv": true, "bytes": true, "encoding/json": true, "io": true, "unicode/utf8": true, "gaigate/internal/provider": true}
	for _, path := range []string{"collector.go", "json.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		require.NoError(t, err)
		for _, imp := range file.Imports {
			name, err := strconv.Unquote(imp.Path.Value)
			require.NoError(t, err)
			require.True(t, allowed[name], "unexpected capability: %s", name)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			_, startsWorker := node.(*ast.GoStmt)
			require.False(t, startsWorker, "collector must not start a worker")
			return true
		})
	}
}
