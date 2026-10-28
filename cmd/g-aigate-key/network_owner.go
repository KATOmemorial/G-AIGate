package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// mysql v1.8.1 exposes custom dialing only through its named registry. Each
// command pool owns a distinct name, never replaces tcp/unix, and unregisters
// only after its Dial/Connect and physical connection owners have completed.
func (c *commandConnector) registerTCP(cfg *mysqlDriver.Config) {
	network := cfg.Net // store.Options.DSN uses tcp, with its normalized address
	dialer := &net.Dialer{Timeout: cfg.Timeout}
	name := fmt.Sprintf("gaigate-key-%p", c)
	c.networkName = name
	c.unregister = func() { mysqlDriver.DeregisterDialContext(name) }
	mysqlDriver.RegisterDialContext(name, func(ctx context.Context, addr string) (net.Conn, error) {
		return c.dialTCP(ctx, network, addr, dialer.DialContext, cfg.Logger)
	})
	cfg.Net = name
}

func (c *commandConnector) dialTCP(ctx context.Context, network, addr string,
	dial func(context.Context, string, string) (net.Conn, error), logger mysqlDriver.Logger,
) (net.Conn, error) {
	dialCtx, finish, err := c.beginCall(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	conn, err := dial(dialCtx, network, addr)
	if err != nil {
		return nil, errors.Join(err, dialCtx.Err(), context.Cause(dialCtx))
	}
	tcp, ok := conn.(*net.TCPConn)
	c.mu.Lock()
	if c.closing || dialCtx.Err() != nil || !ok {
		closing := c.closing
		c.mu.Unlock()
		closeErr := conn.Close()
		c.recordCloseError(closeErr)
		if !ok {
			err = errors.New("command: TCP connection capabilities unavailable")
		}
		if closing {
			err = errors.Join(err, errCommandPoolClosed)
		}
		return nil, errors.Join(err, dialCtx.Err(), context.Cause(dialCtx), closeErr)
	}
	c.networks++
	c.dialed++
	c.owners.Add(1) // Dial's registered owner prevents a zero-count Add/Wait race
	c.mu.Unlock()
	// The driver's *net.TCPConn assertion cannot see through the wrapper. Keep
	// its original keepalive operation explicitly; syscall.Conn and deadlines
	// remain available through the embedded TCPConn (including Linux connCheck).
	if err := tcp.SetKeepAlive(true); err != nil {
		logger.Print(err)
	}
	return &commandTCPConnection{TCPConn: tcp, owner: c}, nil
}

type commandTCPConnection struct {
	*net.TCPConn
	owner     *commandConnector
	closeOnce sync.Once
	closeErr  error
}

func (c *commandTCPConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.TCPConn.Close()
		c.owner.mu.Lock()
		c.owner.closeErr = errors.Join(c.owner.closeErr, c.closeErr)
		c.owner.networks--
		c.owner.networkDone++
		c.owner.mu.Unlock()
		c.owner.owners.Done() // mysql's closed flag can precede this actual close
	})
	return c.closeErr
}
