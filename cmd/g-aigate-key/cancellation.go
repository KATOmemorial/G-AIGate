package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

// A cancellation callback interrupts actual I/O; it never calls owner.Close or
// waits for its own completion. Dormant callbacks are stopped, started callbacks
// are joined, and neither can run after its registration has been released.
type commandCancellation struct {
	owner *commandConnector
	stop  func() bool
	done  chan struct{}
	once  sync.Once
}

func (c *commandConnector) cancelOnContext(ctx context.Context, interrupt func()) (*commandCancellation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return nil, errors.Join(errCommandPoolClosed, ctx.Err(), context.Cause(ctx))
	}
	callback := &commandCancellation{owner: c, done: make(chan struct{})}
	c.callbacks[callback] = struct{}{}
	callback.stop = context.AfterFunc(ctx, func() {
		defer close(callback.done)
		interrupt()
	})
	return callback, nil
}

func (c *commandCancellation) join() {
	c.once.Do(func() {
		if !c.stop() {
			<-c.done
		}
		c.owner.mu.Lock()
		delete(c.owner.callbacks, c)
		c.owner.mu.Unlock()
	})
}

// Seal first; cancel and close outside the lock. Closing the physical sockets
// makes the locked driver's contextless COMMIT/ROLLBACK reads and writes return.
// The existing resource wait still joins the driver and every actual TCP close.
func (c *commandConnector) interrupt() {
	connections := c.seal()
	interruptCommandConnections(connections)
}

func interruptCommandConnections(connections []*commandTCPConnection) {
	for _, connection := range connections {
		_ = connection.Close() // Close records its error in the owning connector.
	}
}

func (c *commandConnector) seal() []*commandTCPConnection {
	c.mu.Lock()
	c.closing = true
	cancels := make([]context.CancelCauseFunc, 0, len(c.connecting))
	for _, cancel := range c.connecting {
		cancels = append(cancels, cancel)
	}
	connections := make([]*commandTCPConnection, 0, len(c.tcp))
	for connection := range c.tcp {
		connections = append(connections, connection)
	}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel(errCommandPoolClosed)
	}
	return connections
}

// Connect and its custom Dial share this private value. A transaction callback
// closes only the physical connection acquired by that Connect, not the pool.
type commandConnectionNetworkKey struct{}
type commandConnectionNetwork struct {
	mu  sync.Mutex
	tcp *commandTCPConnection
}

func (n *commandConnectionNetwork) interrupt() {
	n.mu.Lock()
	connection := n.tcp
	n.mu.Unlock()
	if connection != nil {
		interruptCommandConnections([]*commandTCPConnection{connection})
	}
}

func (c *commandDriverConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	callback, err := c.owner.cancelOnContext(ctx, c.network.interrupt)
	if err != nil {
		return nil, err
	}
	tx, err := c.commandMySQLCapabilities.BeginTx(ctx, opts)
	if err != nil {
		callback.join()
		return nil, err
	}
	return &commandTransaction{Tx: tx, callback: callback}, nil
}

type commandTransaction struct {
	driver.Tx
	callback *commandCancellation
}

func (tx *commandTransaction) Commit() error {
	defer tx.callback.join()
	return tx.Tx.Commit()
}

func (tx *commandTransaction) Rollback() error {
	defer tx.callback.join()
	return tx.Tx.Rollback()
}
