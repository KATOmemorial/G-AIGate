package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/store"
)

func ownerTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.AcceptTCP()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

func TestCommandNetworkOwnerJoinsStartedCancellationAfterSocketClose(t *testing.T) {
	client, server := ownerTCPPair(t)
	physical := ownerTestBase()
	base := &ownerTestConnector{connect: func(context.Context) (driver.Conn, error) { return physical, nil }, closing: make(chan struct{})}
	owner := newCommandConnector(base)
	cfg := mysqlDriver.NewConfig()
	cfg.Net, cfg.Addr, cfg.Logger = "tcp", "127.0.0.1:1", silentDriverLogger{}
	owner.registerTCP(cfg)
	wrapped, err := owner.dialTCP(context.Background(), "tcp", "fixture", func(context.Context, string, string) (net.Conn, error) { return client, nil }, silentDriverLogger{})
	require.NoError(t, err)
	driverConn, err := owner.Connect(context.Background())
	require.NoError(t, err)
	// Models mysql's closed flag: its driver Close already returns, while the
	// cancel watcher is held before invoking the actual raw TCP Close.
	require.NoError(t, driverConn.Close())
	assert.True(t, physical.closed.Load())
	watcherGate, watcherEntered, closeDone, peerDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(watcherGate) }) }
	var closeErr, watcherErr, peerErr error
	go func() { defer close(peerDone); var b [1]byte; _, peerErr = server.Read(b[:]) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callback, err := owner.cancelOnContext(ctx, func() {
		close(watcherEntered)
		<-watcherGate
		watcherErr = wrapped.Close()
	})
	require.NoError(t, err)
	cancel()
	ownerWait(t, watcherEntered, "started cancellation callback")
	watcherDone := callback.done
	go func() { defer close(closeDone); closeErr = owner.Close() }()
	defer func() {
		release()
		ownerWait(t, watcherDone, "watcher fixture joined")
		ownerWait(t, closeDone, "pool owner joined")
		_ = server.Close()
		ownerWait(t, peerDone, "peer fixture joined")
	}()
	ownerWait(t, base.closing, "connector sealed after driver close")
	ownerMustBlock(t, closeDone)
	// The private name must remain installed until existing resources finish;
	// its already-sealed callback rejects the new call without creating a socket.
	registered, err := mysqlDriver.NewConnector(cfg)
	require.NoError(t, err)
	late, err := registered.Connect(context.Background())
	assert.Nil(t, late)
	assert.ErrorIs(t, err, errCommandPoolClosed)
	owner.mu.Lock()
	assert.Zero(t, owner.networks)
	assert.Zero(t, owner.connections)
	owner.mu.Unlock()
	ownerWait(t, peerDone, "Close proactively interrupted actual TCP")
	release()
	ownerWait(t, watcherDone, "actual socket close")
	ownerWait(t, closeDone, "owner joins actual network")
	ownerWait(t, peerDone, "peer observed EOF")
	assert.NoError(t, watcherErr)
	assert.NoError(t, closeErr)
	assert.ErrorIs(t, peerErr, io.EOF)
	assertCommandOwnerClosed(t, owner)
	assertCommandNetworkUnregistered(t, owner)
}

func TestCommandTCPKeepsCapabilitiesAndConcurrentClose(t *testing.T) {
	client, server := ownerTCPPair(t)
	owner := newCommandConnector(&ownerTestConnector{connect: func(context.Context) (driver.Conn, error) { return nil, errors.New("unused") }})
	wrapped, err := owner.dialTCP(context.Background(), "tcp", "fixture", func(context.Context, string, string) (net.Conn, error) { return client, nil }, silentDriverLogger{})
	require.NoError(t, err)
	defer owner.Close()
	defer wrapped.Close()
	sys, ok := wrapped.(syscall.Conn)
	require.True(t, ok, "Linux MySQL connCheck must retain syscall capability")
	raw, err := sys.SyscallConn()
	require.NoError(t, err)
	called := false
	require.NoError(t, raw.Control(func(uintptr) { called = true }))
	assert.True(t, called)
	assert.Equal(t, client.LocalAddr().String(), wrapped.LocalAddr().String())
	assert.Equal(t, client.RemoteAddr().String(), wrapped.RemoteAddr().String())
	require.NoError(t, wrapped.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	var b [1]byte
	_, err = wrapped.Read(b[:])
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	assert.True(t, netErr.Timeout())
	require.NoError(t, wrapped.SetDeadline(time.Time{}))
	_, err = wrapped.Write([]byte{7})
	require.NoError(t, err)
	_, err = io.ReadFull(server, b[:])
	require.NoError(t, err)
	assert.Equal(t, byte(7), b[0])
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- wrapped.Close() }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		assert.NoError(t, err)
	}
	owner.mu.Lock()
	assert.Zero(t, owner.networks)
	assert.Equal(t, 1, owner.networkDone)
	owner.mu.Unlock()
}

func TestCommandNetworkOwnerSealsAndJoinsLateDial(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		name := "canceled_dial"
		if lateSuccess {
			name = "late_socket"
		}
		t.Run(name, func(t *testing.T) {
			client, server := ownerTCPPair(t)
			entered, observed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			base := &ownerTestConnector{closing: make(chan struct{})}
			owner := newCommandConnector(base)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var calls atomic.Int64
			var observedCause error
			dial := func(actual context.Context, network, addr string) (net.Conn, error) {
				calls.Add(1)
				assert.Equal(t, "tcp", network)
				assert.Equal(t, "fixture", addr)
				d, ok := actual.Deadline()
				assert.True(t, ok)
				want, _ := ctx.Deadline()
				assert.Equal(t, want, d)
				close(entered)
				<-actual.Done()
				observedCause = context.Cause(actual)
				close(observed)
				<-release
				if lateSuccess {
					return client, nil
				}
				return nil, actual.Err()
			}
			var gateOnce sync.Once
			unblock := func() { gateOnce.Do(func() { close(release) }) }
			dialDone, closeDone := make(chan struct{}), make(chan struct{})
			var actual net.Conn
			var dialErr, closeErr error
			go func() {
				defer close(dialDone)
				actual, dialErr = owner.dialTCP(ctx, "tcp", "fixture", dial, silentDriverLogger{})
			}()
			defer func() {
				unblock()
				ownerWait(t, dialDone, "Dial fixture joined")
				ownerWait(t, closeDone, "dial owner joined")
			}()
			ownerWait(t, entered, "Dial entry")
			go func() { defer close(closeDone); closeErr = owner.Close() }()
			ownerWait(t, base.closing, "dial admission sealed")
			ownerWait(t, observed, "dial received close cancellation")
			ownerMustBlock(t, closeDone)
			assert.ErrorIs(t, observedCause, errCommandPoolClosed)
			assert.NoError(t, ctx.Err())
			_, err := owner.dialTCP(ctx, "tcp", "fixture", dial, silentDriverLogger{})
			assert.ErrorIs(t, err, errCommandPoolClosed)
			assert.Equal(t, int64(1), calls.Load())
			unblock()
			ownerWait(t, dialDone, "late Dial completed")
			ownerWait(t, closeDone, "late resource disposed")
			assert.Nil(t, actual)
			assert.ErrorIs(t, dialErr, context.Canceled)
			assert.ErrorIs(t, dialErr, errCommandPoolClosed)
			assert.NoError(t, closeErr)
			if lateSuccess {
				require.NoError(t, server.SetReadDeadline(time.Now().Add(time.Second)))
				var b [1]byte
				_, err = server.Read(b[:])
				assert.ErrorIs(t, err, io.EOF)
			}
			owner.mu.Lock()
			assert.True(t, owner.closed)
			assert.Empty(t, owner.connecting)
			assert.Zero(t, owner.networks)
			owner.mu.Unlock()
		})
	}
}

func assertCommandNetworkUnregistered(t *testing.T, c *commandConnector) {
	t.Helper()
	cfg := mysqlDriver.NewConfig()
	cfg.Net = c.networkName
	cfg.Addr = "127.0.0.1:1"
	cfg.Logger = silentDriverLogger{}
	connector, err := mysqlDriver.NewConnector(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx)
	assert.Nil(t, conn)
	var unknown net.UnknownNetworkError
	assert.ErrorAs(t, err, &unknown, "only this private dial name must have been removed")
}

func TestCommandNetworkRegistrationFailureCleanup(t *testing.T) {
	owner := newCommandConnector(nil)
	cfg := mysqlDriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = "127.0.0.1:1"
	cfg.Logger = silentDriverLogger{}
	owner.registerTCP(cfg)
	name := owner.networkName
	require.NotEmpty(t, name)
	assert.NotEqual(t, "tcp", name)
	require.NoError(t, owner.Close())
	assertCommandNetworkUnregistered(t, owner)
	_, err := owner.dialTCP(context.Background(), "tcp", "fixture", func(context.Context, string, string) (net.Conn, error) {
		t.Error("dial invoked after unregister")
		return nil, errors.New("unexpected")
	}, silentDriverLogger{})
	assert.ErrorIs(t, err, errCommandPoolClosed)
}

func TestCommandMySQLFailedHandshakeUnregistersTCP(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	done := make(chan struct{})
	var peerErr error
	go func() {
		defer close(done)
		conn, err := listener.AcceptTCP()
		if err != nil {
			peerErr = err
			return
		}
		defer conn.Close()
		_, peerErr = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		ownerWait(t, done, "silent handshake peer joined")
		assert.NoError(t, peerErr)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	db, owner, err := openMySQLWithOwner(ctx, store.Options{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, Database: "gaigate_test"})
	assert.Nil(t, db)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, owner)
	owner.mu.Lock()
	assert.True(t, owner.closed)
	assert.Empty(t, owner.connecting)
	assert.Zero(t, owner.networks)
	assert.Equal(t, 1, owner.dialed)
	assert.Equal(t, owner.dialed, owner.networkDone)
	owner.mu.Unlock()
	assertCommandNetworkUnregistered(t, owner)
}
