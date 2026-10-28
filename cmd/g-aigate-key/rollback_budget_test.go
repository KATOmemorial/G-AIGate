package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/config"
	"gaigate/internal/store"
)

// Relay genuine MySQL packets unchanged, except a ROLLBACK frame waits before
// forwarding. A separate client reader still observes EOF and closes upstream,
// so cancellation must work without the test releasing the ROLLBACK barrier.
type rollbackTransportProxy struct {
	listener                             *net.TCPListener
	ctx                                  context.Context
	stop                                 context.CancelFunc
	opts                                 store.Options
	held, release, peerEOF, acceptedDone chan struct{}
	heldOnce, releaseOnce, eofOnce       sync.Once
	mu                                   sync.Mutex
	sessions                             map[net.Conn]net.Conn
	workers                              sync.WaitGroup
}

func newRollbackTransportProxy(t *testing.T, opts store.Options) *rollbackTransportProxy {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	ctx, stop := context.WithCancel(context.Background())
	p := &rollbackTransportProxy{listener: l, ctx: ctx, stop: stop, opts: opts, held: make(chan struct{}), release: make(chan struct{}), peerEOF: make(chan struct{}), acceptedDone: make(chan struct{}), sessions: make(map[net.Conn]net.Conn)}
	go func() {
		defer close(p.acceptedDone)
		for {
			client, err := l.AcceptTCP()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.sessions[client] = nil
			p.mu.Unlock()
			p.workers.Add(1)
			go p.serve(client)
		}
	}()
	t.Cleanup(p.Close)
	return p
}

func (p *rollbackTransportProxy) serve(client net.Conn) {
	defer p.workers.Done()
	defer func() { _ = client.Close(); p.mu.Lock(); delete(p.sessions, client); p.mu.Unlock() }()
	upstream, err := (&net.Dialer{}).DialContext(p.ctx, "tcp", net.JoinHostPort(p.opts.Host, strconv.Itoa(p.opts.Port)))
	if err != nil {
		return
	}
	defer upstream.Close()
	p.mu.Lock()
	p.sessions[client] = upstream
	p.mu.Unlock()
	if p.ctx.Err() != nil {
		return
	}
	packets := make(chan []byte, 1)
	readerDone, reverseDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(packets)
		defer upstream.Close()
		for {
			var header [4]byte
			if _, err := io.ReadFull(client, header[:]); err != nil {
				if errors.Is(err, io.EOF) {
					p.eofOnce.Do(func() { close(p.peerEOF) })
				}
				return
			}
			length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
			packet := make([]byte, 4+length)
			copy(packet, header[:])
			if _, err := io.ReadFull(client, packet[4:]); err != nil {
				return
			}
			select {
			case packets <- packet:
			case <-p.ctx.Done():
				return
			}
		}
	}()
	go func() { defer close(reverseDone); _, _ = io.Copy(client, upstream) }()
	for packet := range packets {
		if len(packet) > 5 && packet[4] == 3 && string(packet[5:]) == "ROLLBACK" {
			p.heldOnce.Do(func() { close(p.held) })
			select {
			case <-p.release:
			case <-readerDone:
			case <-p.ctx.Done():
			}
			select {
			case <-readerDone:
				break
			default:
				if _, err = upstream.Write(packet); err == nil {
					continue
				}
			}
			break
		}
		if _, err = upstream.Write(packet); err != nil {
			break
		}
	}
	_ = client.Close()
	_ = upstream.Close()
	<-readerDone
	<-reverseDone
}

func (p *rollbackTransportProxy) Release() { p.releaseOnce.Do(func() { close(p.release) }) }
func (p *rollbackTransportProxy) Close() {
	p.stop()
	_ = p.listener.Close()
	<-p.acceptedDone
	p.mu.Lock()
	connections := make([]net.Conn, 0, 2*len(p.sessions))
	for client, up := range p.sessions {
		connections = append(connections, client)
		if up != nil {
			connections = append(connections, up)
		}
	}
	p.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	p.workers.Wait()
}
func (p *rollbackTransportProxy) Options() store.Options {
	opts := p.opts
	opts.Host = "127.0.0.1"
	opts.Port = p.listener.Addr().(*net.TCPAddr).Port
	return opts
}

func captureCommandConnection(t *testing.T, ctx context.Context, pool *sql.DB) *commandDriverConnection {
	t.Helper()
	pool.SetMaxOpenConns(1)
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	var actual *commandDriverConnection
	require.NoError(t, conn.Raw(func(driverConn any) error { actual = driverConn.(*commandDriverConnection); return nil }))
	require.NoError(t, conn.Close())
	return actual
}

func TestCommandRollbackCancellationBudget(t *testing.T) {
	fixture, opts := commandTestDB(t, true)
	user := commandTestUser(t, fixture)
	for _, foreground := range []bool{false, true} {
		for _, activeCause := range []bool{false, true} {
			name := "background/deadline"
			if foreground {
				name = "foreground/deadline"
			}
			if activeCause {
				if foreground {
					name = "foreground/cause"
				} else {
					name = "background/cause"
				}
			}
			t.Run(name, func(t *testing.T) {
				proxy := newRollbackTransportProxy(t, opts)
				parent, stop := context.WithTimeout(context.Background(), 400*time.Millisecond)
				defer stop()
				ctx, cancel := context.WithCancelCause(parent)
				defer cancel(nil)
				cause := errors.New("controlled cancellation during genuine ROLLBACK")
				var canceledAt atomic.Pointer[time.Time]
				deadline, _ := parent.Deadline()
				canceledAt.Store(&deadline)
				cancelWithCause := func() { when := time.Now(); canceledAt.Store(&when); cancel(cause) }
				var actualOwner *commandConnector
				var serverID uint64
				deps := commandDeps{load: func(string) (*config.Config, error) { return &config.Config{}, nil }, open: func(commandCtx context.Context, _ store.Options) (*keyDatabase, error) {
					db, owner, err := openMySQLWithOwner(commandCtx, proxy.Options())
					if err != nil {
						return nil, err
					}
					actualOwner = owner
					pool, err := db.DB()
					if err != nil {
						return nil, err
					}
					physical := captureCommandConnection(t, commandCtx, pool)
					require.NoError(t, db.Raw("SELECT CONNECTION_ID()").Scan(&serverID).Error)
					err = db.Callback().Create().Before("gorm:commit_or_rollback_transaction").Register("134:rollback_budget", func(tx *gorm.DB) {
						if tx.Statement.Table != "api_keys" || tx.Error != nil {
							return
						}
						if foreground {
							tx.AddError(errors.New("controlled failure before commit"))
							_ = tx.Rollback()
							return
						}
						// Hold only private bookkeeping locks, so both cancellation
						// callbacks wait while native awaitDone issues genuine ROLLBACK.
						// Neither the SQL library nor the driver is replaced or patched.
						owner.mu.Lock()
						physical.network.mu.Lock()
						defer owner.mu.Unlock()
						defer physical.network.mu.Unlock()
						if activeCause {
							cancelWithCause()
						} else {
							<-commandCtx.Done()
						}
						ownerWait(t, proxy.held, "background genuine ROLLBACK transport entry")
					})
					if err != nil {
						return nil, err
					}
					users := store.NewUserRepo(db)
					return &keyDatabase{users: users, service: access.NewKeyService(store.NewAPIKeyRepo(db), users), close: pool.Close}, nil
				}}
				done := make(chan struct{})
				var code int
				var finishedAt time.Time
				var out, errOut bytes.Buffer
				go func() {
					defer close(done)
					code = run(ctx, []string{"-user", user.Username}, &out, &errOut, deps)
					finishedAt = time.Now()
				}()
				defer func() { proxy.Release(); ownerWait(t, done, "command cleanup after transport release"); proxy.Close() }()
				ownerWait(t, proxy.held, "genuine ROLLBACK received by transparent proxy")
				if activeCause && foreground {
					cancelWithCause()
				}
				<-ctx.Done()
				select {
				case <-done:
					t.Logf("genuine ROLLBACK returned %s after original cancellation without forwarding barrier release", finishedAt.Sub(*canceledAt.Load()))
				case <-time.After(time.Second):
					t.Error("command did not interrupt genuine ROLLBACK within cancellation budget")
				}
				// Release only after the bounded-return assertion; a mutation must
				// fail above but still let all native/proxy owners finish in cleanup.
				proxy.Release()
				ownerWait(t, done, "command finished")
				ownerWait(t, proxy.peerEOF, "actual command TCP peer EOF")
				assert.Equal(t, 1, code)
				assert.Empty(t, out.String())
				assert.NotContains(t, errOut.String(), cause.Error())
				if activeCause {
					assert.ErrorIs(t, context.Cause(ctx), cause)
				} else {
					assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
				}
				assertCommandOwnerClosed(t, actualOwner)
				assertCommandNetworkUnregistered(t, actualOwner)
				proxy.Close()
				require.Eventually(t, func() bool {
					var n int64
					err := fixture.Raw("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = ?", serverID).Scan(&n).Error
					return err == nil && n == 0
				}, time.Second, 10*time.Millisecond, "proxy upstream session must exit after EOF")
				var count int64
				require.NoError(t, fixture.Model(&store.APIKey{}).Count(&count).Error)
				assert.Zero(t, count)
			})
		}
	}
}

func TestCommandFinishedTransactionCannotAbortReusedConnection(t *testing.T) {
	fixture, opts := commandTestDB(t, true)
	user := commandTestUser(t, fixture)
	ctx, stop := context.WithTimeout(context.Background(), commandTimeout)
	defer stop()
	db, owner, err := openMySQLWithOwner(ctx, opts)
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	for _, rollback := range []bool{false, true} {
		caller, cancel := context.WithCancelCause(context.Background())
		var before, after uint64
		require.NoError(t, db.Raw("SELECT CONNECTION_ID()").Scan(&before).Error)
		controlled := errors.New("controlled normal rollback")
		err = db.WithContext(caller).Transaction(func(tx *gorm.DB) error {
			_, err := store.NewUserRepo(tx).Get(caller, int64(user.ID))
			if err != nil {
				return err
			}
			if rollback {
				return controlled
			}
			return nil
		})
		if rollback {
			require.ErrorIs(t, err, controlled)
		} else {
			require.NoError(t, err)
		}
		owner.mu.Lock()
		assert.Len(t, owner.callbacks, 1, "only command lifecycle callback may remain")
		owner.mu.Unlock()
		cancel(errors.New("old transaction ctx canceled after connection reuse"))
		require.NoError(t, db.Raw("SELECT CONNECTION_ID()").Scan(&after).Error)
		assert.Equal(t, before, after)
		require.NoError(t, ctx.Err())
		owner.mu.Lock()
		assert.False(t, owner.closing)
		owner.mu.Unlock()
	}
	require.NoError(t, pool.Close())
	assertCommandOwnerClosed(t, owner)
	assertCommandNetworkUnregistered(t, owner)
}
