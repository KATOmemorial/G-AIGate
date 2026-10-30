package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandCancellationStopPreventsLateCallback(t *testing.T) {
	owner := newCommandConnector(&ownerTestConnector{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var called atomic.Bool
	callback, err := owner.cancelOnContext(ctx, func() { called.Store(true) })
	require.NoError(t, err)
	callback.join()
	callback.join() // stopped callback has no done signal to wait for
	cancel()
	require.NoError(t, owner.Close())
	assert.False(t, called.Load())
	owner.mu.Lock()
	assert.Empty(t, owner.callbacks)
	assert.True(t, owner.closed)
	owner.mu.Unlock()
}

func TestCommandCancellationConcurrentJoinWaitsForStartedCallback(t *testing.T) {
	base := &ownerTestConnector{closing: make(chan struct{})}
	owner := newCommandConnector(base)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	callback, err := owner.cancelOnContext(ctx, func() { close(entered); <-release })
	require.NoError(t, err)
	cancel()
	ownerWait(t, entered, "owned callback started")
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var wg sync.WaitGroup
	done := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) { defer wg.Done(); defer close(done[index]); callback.join() }(i)
	}
	wg.Add(1)
	go func() { defer wg.Done(); defer close(done[2]); assert.NoError(t, owner.Close()) }()
	defer func() { unblock(); wg.Wait(); ownerWait(t, callback.done, "started cancellation cleanup") }()
	ownerWait(t, base.closing, "owner sealed before joining callbacks")
	for _, ch := range done {
		ownerMustBlock(t, ch)
	}
	owner.mu.Lock()
	assert.Len(t, owner.callbacks, 1)
	owner.mu.Unlock()
	unblock()
	for _, ch := range done {
		ownerWait(t, ch, "concurrent callback/owner join")
	}
	owner.mu.Lock()
	assert.Empty(t, owner.callbacks)
	assert.True(t, owner.closed)
	owner.mu.Unlock()
}
