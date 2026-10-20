package store

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 审查②：真实 TCP 连接接收命令但不回复，既覆盖首连握手，也覆盖已建立的连接。
func TestNewRedis_PingHonorsDeadlineOnUnresponsiveServer(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "initial_handshake"
		if warm {
			name = "pooled_connection"
		}
		t.Run(name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := NewRedis(RedisOptions{Addr: mr.Addr(), DB: 3})
			t.Cleanup(func() { _ = client.Close() })
			if warm {
				require.NoError(t, client.Ping(context.Background()).Err())
			}
			received := make(chan string, 1)
			mr.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
				select {
				case received <- cmd:
				default:
				}
				return true // 已消费命令，故意不写任何协议响应。
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := client.Ping(ctx).Err()
			elapsed := time.Since(start)
			var timeout net.Error
			require.ErrorAs(t, err, &timeout)
			assert.True(t, timeout.Timeout())
			assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond)
			assert.Less(t, elapsed, 600*time.Millisecond, "100ms context 应约束网络读取，而非等待默认 3s")
			select {
			case cmd := <-received:
				want := "HELLO"
				if warm {
					want = "PING"
				}
				assert.Equal(t, want, strings.ToUpper(cmd), "确认已建立连接且服务器实际收到命令")
			default:
				t.Fatal("服务器未收到命令，未命中网络读超时场景")
			}
			t.Logf("elapsed=%s err=%v", elapsed, err)
		})
	}
}
