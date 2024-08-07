// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TokenCounter is the interface for a token counter that can be used to deduct
// and return flow control tokens. Additionally, it can be used to wait for
// tokens to become available, and to check if tokens are available without
// blocking.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type TokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it returns
	// a handle that may be used for waiting for tokens to become available.
	TokensAvailable() (available bool, tokenWaitingHandle TokenWaitingHandle)
	// TryDeduct attempts to deduct flow tokens for the given work class. If
	// there are no tokens available, 0 tokens are returned. When less than the
	// requested token count is available, partial tokens are returned
	// corresponding to this partial amount.
	TryDeduct(context.Context, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	// Deduct deducts (without blocking) flow tokens for the given work class. If
	// there are not enough available tokens, the token counter will go into debt
	// (negative available count) and still issue the requested number of tokens.
	Deduct(context.Context, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given work class.
	Return(context.Context, kvflowcontrol.Tokens)
}

// TokenWaitingHandle is the interface for waiting for positive tokens from a
// token counter.
type TokenWaitingHandle interface {
	// WaitChannel is the channel that will be signaled if tokens are possibly
	// available. If signaled, the caller must call
	// ConfirmHaveTokensAndUnblockNextWaiter. There is no guarantee of tokens
	// being available after this channel is signaled, just that tokens were
	// available recently. A typical usage pattern is:
	//
	//   for {
	//     select {
	//     case <-handle.WaitChannel():
	//       if handle.ConfirmHaveTokensAndUnblockNextWaiter() {
	//         break
	//       }
	//     }
	//   }
	//   tokenCounter.Deduct(...)
	//
	// There is a possibility for races, where multiple goroutines may be
	// signaled and deduct tokens, sending the counter into debt. These cases are
	// acceptable, as in aggregate the counter provides pacing over time.
	WaitChannel() <-chan struct{}
	// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
	// available. True is returned if tokens are available, false otherwise. If
	// no tokens are available, the caller can resume waiting using WaitChannel.
	ConfirmHaveTokensAndUnblockNextWaiter() bool
}

func (bwc *tokenCounter) adjustTokensLocked(
	ctx context.Context, delta kvflowcontrol.Tokens, limit kvflowcontrol.Tokens, admin bool,
) {
	var unaccounted kvflowcontrol.Tokens
	before := bwc.mu.tokens
	bwc.mu.tokens += delta
	if delta > 0 {
		if bwc.mu.tokens > limit {
			unaccounted = bwc.mu.tokens - limit
			bwc.mu.tokens = limit
		}
		if before <= 0 && bwc.mu.tokens > 0 {
			bwc.signal()
		}
	}
	if buildutil.CrdbTestBuild && !admin && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d",
			unaccounted, delta, limit)
	}
}

func (bwc *tokenCounter) signal() {
	select {
	// Non-blocking channel write that ensures it's topped up to 1 entry.
	case bwc.signalCh <- struct{}{}:
	default:
	}
}

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type tokenCounter struct {
	settings *cluster.Settings

	// Waiting requests do so by waiting on signalCh without holding a mutex.
	//
	// Requests first check for available tokens (by acquiring and releasing the
	// mutex), and then wait if tokens for their work class are unavailable. The
	// risk in such waiting after releasing the mutex is the following race:
	// tokens become available after the waiter releases the mutex and before it
	// starts waiting. We handle this race by ensuring that signalCh always has
	// an entry if tokens are available:
	//
	// - Whenever tokens are returned, signalCh is signaled, waking up a single
	//   waiting request. If the request finds no available tokens, it starts
	//   waiting again.
	// - Whenever a request gets admitted, it signals the next waiter if any.
	//
	// So at least one request that observed unavailable tokens will get
	// unblocked, which will in turn unblock others. This turn by turn admission
	// provides some throttling to over-admission since the goroutine scheduler
	// needs to schedule the goroutine that got the entry for it to unblock
	// another.
	signalCh chan struct{}
	mu       struct {
		syncutil.RWMutex

		// Token limit per work class, tracking
		// kvadmission.flow_controller.{regular,elastic}_tokens_per_stream.
		limit  kvflowcontrol.Tokens
		tokens kvflowcontrol.Tokens
	}
}

var _ TokenCounter = &tokenCounter{}

func newTokenCounter(settings *cluster.Settings) *tokenCounter {
	b := &tokenCounter{
		settings: settings,
	}

	elasticTokens := kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV))
	b.mu.limit = elasticTokens
	b.mu.tokens = b.mu.limit
	b.signalCh = make(chan struct{}, 1)

	onChangeFunc := func(ctx context.Context) {
		b.mu.Lock()
		defer b.mu.Unlock()

		before := b.mu.limit
		after := kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV))
		adjustment := after - before
		b.mu.limit = after

		b.adjustTokensLocked(
			ctx, adjustment, after, true /* admin */)
	}

	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	return b
}

func (b *tokenCounter) tokens() kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked()
}

func (b *tokenCounter) tokensLocked() kvflowcontrol.Tokens {
	return b.mu.tokens
}

// TokensAvailable returns true if tokens are available. If false, it returns
// a handle that may be used for waiting for tokens to become available.
func (b *tokenCounter) TokensAvailable() (available bool, handle TokenWaitingHandle) {
	if b.tokens() > 0 {
		return true, nil
	}
	return false, waitHandle{b: b}
}

// TryDeduct attempts to deduct flow tokens for the given work class. If there
// are no tokens available, 0 tokens are returned. When less than the requested
// token count is available, partial tokens are returned corresponding to this
// partial amount.
func (b *tokenCounter) TryDeduct(
	ctx context.Context, tokens kvflowcontrol.Tokens,
) kvflowcontrol.Tokens {
	b.mu.Lock()
	defer b.mu.Unlock()

	tokensAvailable := b.tokensLocked()
	if tokensAvailable <= 0 {
		return 0
	}

	adjust := -min(tokensAvailable, tokens)
	b.adjustLocked(ctx, adjust, false /* admin */)
	return -adjust
}

// Deduct deducts (without blocking) flow tokens for the given work class. If
// there are not enough available tokens, the token counter will go into debt
// (negative available count) and still issue the requested number of tokens.
func (b *tokenCounter) Deduct(
	ctx context.Context, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, -tokens, false /* admin */)
}

// Return returns flow tokens for the given work class.
func (b *tokenCounter) Return(
	ctx context.Context, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, tokens, false /* admin */)
}

// waitHandle is a handle for waiting for tokens to become available from a
// token counter.
type waitHandle struct {
	b *tokenCounter
}

var _ TokenWaitingHandle = waitHandle{}

// WaitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call
// ConfirmHaveTokensAndUnblockNextWaiter. There is no guarantee of tokens being
// available after this channel is signaled, just that tokens were available
// recently. A typical usage pattern is:
//
//	for {
//	  select {
//	  case <-handle.WaitChannel():
//	    if handle.ConfirmHaveTokensAndUnblockNextWaiter() {
//	      break
//	    }
//	  }
//	}
//	tokenCounter.Deduct(...)
//
// There is a possibility for races, where multiple goroutines may be signaled
// and deduct tokens, sending the counter into debt. These cases are
// acceptable, as in aggregate the counter provides pacing over time.
func (wh waitHandle) WaitChannel() <-chan struct{} {
	return wh.b.signalCh
}

// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
// available. True is returned if tokens are available, false otherwise. If no
// tokens are available, the caller can resume waiting using WaitChannel.
func (wh waitHandle) ConfirmHaveTokensAndUnblockNextWaiter() (haveTokens bool) {
	defer func() {
		// Signal the next waiter if we have tokens available upon returning.
		if haveTokens {
			wh.b.signal()
		}
	}()

	return wh.b.tokens() > 0
}

// adjust the tokens for the given work class by delta. The adjustment is
// performed atomically. When admin is set to true when this method is called
// because of a settings change. In that case the class is interpreted narrowly
// as only updating the tokens for that class.
func (b *tokenCounter) adjust(
	ctx context.Context, delta kvflowcontrol.Tokens, admin bool,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.adjustLocked(ctx, delta, admin)
}

func (b *tokenCounter) adjustLocked(
	ctx context.Context, delta kvflowcontrol.Tokens, admin bool,
) {
	// Elastic {deductions,returns} only affect elastic flow tokens.
	b.adjustTokensLocked(
		ctx, delta, b.mu.limit, admin)
}

// TODO: rename/remove
func (b *tokenCounter) testingGetLimit() kvflowcontrol.Tokens {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.limit
}
