package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/store"
)

// A small MySQL protocol peer exercises the actual driver, not a fake dialer.
// It stops at an observed phase and only exits when the client closes the wire.
func blockedMySQLPeer(t *testing.T, phase string) (store.Options, <-chan struct{}, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	entered, done := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	accepted := make(chan net.Conn, 1)
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		accepted <- conn
		defer conn.Close()
		waitForClose := func() {
			close(entered)
			var b [1]byte
			_, err := conn.Read(b[:])
			if errors.Is(err, io.EOF) {
				result <- nil
			} else {
				result <- errors.New("peer did not observe clean EOF")
			}
		}
		if phase == "handshake" {
			waitForClose()
			return
		}
		if err := writeMySQLPacket(conn, 0, mysqlGreeting()); err != nil {
			result <- err
			return
		}
		if _, err := readMySQLPacket(conn); err != nil {
			result <- err
			return
		}
		if err := writeMySQLPacket(conn, 2, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
			result <- err
			return
		}
		query, err := readMySQLPacket(conn)
		if err != nil {
			result <- err
			return
		}
		// Options.DSN preserves charset=utf8mb4; the real driver applies it
		// after authentication before handing the connection to the query.
		if len(query) > 1 && query[0] == 3 && string(query[1:]) == "SET NAMES utf8mb4" {
			if err := writeMySQLPacket(conn, 1, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
				result <- err
				return
			}
			query, err = readMySQLPacket(conn)
			if err != nil {
				result <- err
				return
			}
		}
		if len(query) < 2 || query[0] != 3 || string(query[1:]) != "SELECT VERSION()" {
			result <- errors.New("version query not observed")
			return
		}
		if phase == "version" {
			waitForClose()
			return
		}
		if err := writeVersionResult(conn); err != nil {
			result <- err
			return
		}
		ping, err := readMySQLPacket(conn)
		if err != nil {
			result <- err
			return
		}
		if len(ping) != 1 || ping[0] != 14 {
			result <- errors.New("explicit ping not observed")
			return
		}
		waitForClose()
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("MySQL fixture did not exit")
		}
	})
	return store.Options{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, User: "fixture", Database: "gaigate_test"}, entered, result
}

func mysqlGreeting() []byte {
	capability := uint32(1 | 8 | 512 | 8192 | 32768 | 524288)
	p := append([]byte{10}, []byte("8.4.11\x00")...)
	p = append(p, 1, 0, 0, 0)
	p = append(p, []byte("12345678")...)
	p = append(p, 0, byte(capability), byte(capability>>8), 45, 2, 0, byte(capability>>16), byte(capability>>24), 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, []byte("abcdefghijkl\x00mysql_native_password\x00")...)
	return p
}

func writeMySQLPacket(conn net.Conn, sequence byte, payload []byte) error {
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}
	if _, err := conn.Write(append(header, payload...)); err != nil {
		return err
	}
	return nil
}

func readMySQLPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if length > 4096 {
		return nil, errors.New("unexpected large fixture packet")
	}
	p := make([]byte, length)
	_, err := io.ReadFull(conn, p)
	return p, err
}

func writeVersionResult(conn net.Conn) error {
	column := make([]byte, 0)
	for _, s := range []string{"def", "", "", "", "VERSION()", ""} {
		column = append(column, byte(len(s)))
		column = append(column, []byte(s)...)
	}
	column = append(column, 12, 33, 0)
	column = binary.LittleEndian.AppendUint32(column, 255)
	column = append(column, 253, 0, 0, 0, 0, 0)
	packets := [][]byte{{1}, column, {254, 0, 0, 2, 0}, append([]byte{6}, []byte("8.4.11")...), {254, 0, 0, 2, 0}}
	for i, p := range packets {
		if err := writeMySQLPacket(conn, byte(i+1), p); err != nil {
			return err
		}
	}
	return nil
}

func TestOpenMySQLWireCancellationAtEachInitializationPhase(t *testing.T) {
	for _, phase := range []string{"handshake", "version", "ping"} {
		for _, mode := range []string{"deadline", "parent cause"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				opts, entered, peerClosed := blockedMySQLPeer(t, phase)
				parent, cancelCause := context.WithCancelCause(context.Background())
				defer cancelCause(nil)
				ctx, cancel := context.WithTimeout(parent, 250*time.Millisecond)
				defer cancel()
				cause := errors.New("controlled parent cancellation")
				cancelDone := make(chan struct{})
				go func() {
					defer close(cancelDone)
					select {
					case <-entered:
						if mode == "parent cause" {
							cancelCause(cause)
						}
					case <-ctx.Done():
					}
				}()
				start := time.Now()
				db, err := openMySQL(ctx, opts)
				elapsed := time.Since(start)
				<-cancelDone
				assert.Nil(t, db)
				require.Error(t, err)
				select {
				case <-entered:
				default:
					t.Fatal("test did not reach intended protocol phase")
				}
				if mode == "deadline" {
					assert.ErrorIs(t, err, context.DeadlineExceeded)
					assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
				} else {
					assert.ErrorIs(t, err, context.Canceled)
					assert.ErrorIs(t, err, cause)
				}
				assert.Less(t, elapsed, 2*time.Second)
				t.Logf("phase=%s mode=%s elapsed=%s; phase entered and connection close checked", phase, mode, elapsed.Round(time.Millisecond))
				select {
				case err := <-peerClosed:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("driver connection remained open")
				}
			})
		}
	}
}
