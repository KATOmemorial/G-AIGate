package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
)

var errCommandPoolClosed = errors.New("command: connection owner closed")

// These are all optional connection interfaces implemented by locked mysql
// v1.8.1. Embedding this interface forwards them unchanged, including caller
// contexts, transaction options, legacy operations and value conversion.
type commandMySQLCapabilities interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	Exec(string, []driver.Value) (driver.Result, error)
	driver.ExecerContext
	Query(string, []driver.Value) (driver.Rows, error)
	driver.QueryerContext
	driver.Pinger
	driver.NamedValueChecker
	driver.SessionResetter
	driver.Validator
}

// sql.DB.Close calls this connector's Close after sealing its own admission.
// Unlike DB's counters or sql.Conn's done CAS, owners joins actual driver closes.
// This adapter belongs only to the local command, not the shared store opener.
type commandConnector struct {
	driver.Connector
	mu          sync.Mutex
	closing     bool
	closed      bool
	next        uint64
	connecting  map[uint64]context.CancelCauseFunc
	connections int
	acquired    int
	released    int
	networks    int
	dialed      int
	networkDone int
	networkName string
	unregister  func()
	closeErr    error
	owners      sync.WaitGroup
	closeOnce   sync.Once
	tcp         map[*commandTCPConnection]struct{}
	callbacks   map[*commandCancellation]struct{}
}

func newCommandConnector(connector driver.Connector) *commandConnector {
	return &commandConnector{Connector: connector, connecting: make(map[uint64]context.CancelCauseFunc),
		tcp: make(map[*commandTCPConnection]struct{}), callbacks: make(map[*commandCancellation]struct{})}
}

func (c *commandConnector) beginCall(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, errors.Join(err, context.Cause(ctx))
	}
	connectCtx, cancel := context.WithCancelCause(ctx)
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		cancel(nil)
		return nil, nil, errors.Join(errCommandPoolClosed, ctx.Err(), context.Cause(ctx))
	}
	c.next++
	id := c.next
	c.connecting[id] = cancel
	c.owners.Add(1) // registered under admission lock, before Close may Wait
	c.mu.Unlock()
	return connectCtx, func() {
		cancel(nil)
		c.mu.Lock()
		delete(c.connecting, id)
		c.mu.Unlock()
		c.owners.Done()
	}, nil
}

func (c *commandConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connectCtx, finish, err := c.beginCall(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	network := &commandConnectionNetwork{}
	connectCtx = context.WithValue(connectCtx, commandConnectionNetworkKey{}, network)
	conn, err := c.Connector.Connect(connectCtx)
	if err != nil {
		return nil, errors.Join(err, connectCtx.Err(), context.Cause(connectCtx))
	}
	capabilities, ok := conn.(commandMySQLCapabilities)
	c.mu.Lock()
	if c.closing || connectCtx.Err() != nil || !ok {
		closing := c.closing
		c.mu.Unlock()
		closeErr := conn.Close() // late/unsupported connection is still this Connect's owner
		c.recordCloseError(closeErr)
		if !ok {
			err = errors.New("command: MySQL connection capabilities unavailable")
		}
		if closing {
			err = errors.Join(err, errCommandPoolClosed)
		}
		return nil, errors.Join(err, connectCtx.Err(), context.Cause(connectCtx), closeErr)
	}
	c.connections++
	c.acquired++
	c.owners.Add(1) // Connect's owner is still live, so Wait cannot cross zero
	c.mu.Unlock()
	return &commandDriverConnection{commandMySQLCapabilities: capabilities, owner: c, network: network}, nil
}

func (c *commandConnector) recordCloseError(err error) {
	if err != nil {
		c.mu.Lock()
		c.closeErr = errors.Join(c.closeErr, err)
		c.mu.Unlock()
	}
}

func (c *commandConnector) Close() error {
	c.closeOnce.Do(func() {
		connections := c.seal()
		if closer, ok := c.Connector.(io.Closer); ok {
			c.recordCloseError(closer.Close())
		}
		interruptCommandConnections(connections)
		c.owners.Wait()
		c.mu.Lock()
		callbacks := make([]*commandCancellation, 0, len(c.callbacks))
		for callback := range c.callbacks {
			callbacks = append(callbacks, callback)
		}
		c.mu.Unlock()
		for _, callback := range callbacks {
			callback.join()
		}
		if c.unregister != nil {
			c.unregister() // no Dial/Connect/connection owner can still use this name
		}
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
	}) // concurrent Close calls join the same actual completion
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

type commandDriverConnection struct {
	commandMySQLCapabilities
	owner     *commandConnector
	closeOnce sync.Once
	closeErr  error
	network   *commandConnectionNetwork
}

func (c *commandDriverConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.commandMySQLCapabilities.Close()
		c.owner.mu.Lock()
		c.owner.closeErr = errors.Join(c.owner.closeErr, c.closeErr)
		c.owner.connections--
		c.owner.released++
		c.owner.mu.Unlock()
		c.owner.owners.Done() // actual driver Close returned; SQL metadata may follow
	})
	return c.closeErr
}
