// Copyright 2026 Lolia Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fatedier/golib/pool"
)

// wrappedConn hides the concrete *net.TCPConn type so Join takes the
// adaptive userspace path instead of the splice fast path.
type wrappedConn struct {
	net.Conn
}

func tcpPair(tb testing.TB) (*net.TCPConn, *net.TCPConn) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	defer ln.Close()

	dialed, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(tb, err)
	accepted, err := ln.Accept()
	require.NoError(tb, err)

	c1 := dialed.(*net.TCPConn)
	c2 := accepted.(*net.TCPConn)
	deadline := time.Now().Add(30 * time.Second)
	require.NoError(tb, c1.SetDeadline(deadline))
	require.NoError(tb, c2.SetDeadline(deadline))
	tb.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	return c1, c2
}

func randBytes(tb testing.TB, n int) []byte {
	tb.Helper()
	buf := make([]byte, n)
	_, err := rand.Read(buf)
	require.NoError(tb, err)
	return buf
}

// testJoinIntegrity pushes data through appA <-> Join <-> appB in both
// directions at once and verifies content and reported byte counts.
func testJoinIntegrity(t *testing.T, wrap bool) {
	t.Helper()
	a1, a2 := tcpPair(t)
	b1, b2 := tcpPair(t)

	var joinC1, joinC2 io.ReadWriteCloser = a2, b1
	if wrap {
		joinC1, joinC2 = wrappedConn{a2}, wrappedConn{b1}
	}

	var inCount, outCount int64
	joinDone := make(chan struct{})
	go func() {
		defer close(joinDone)
		inCount, outCount, _ = Join(joinC1, joinC2)
	}()

	dataA := randBytes(t, 8*1024*1024)
	dataB := randBytes(t, 512*1024)

	var wg sync.WaitGroup
	wg.Add(4)
	var gotA, gotB []byte
	var errW1, errW2, errR1, errR2 error
	go func() {
		defer wg.Done()
		_, errW1 = a1.Write(dataA)
	}()
	go func() {
		defer wg.Done()
		_, errW2 = b2.Write(dataB)
	}()
	go func() {
		defer wg.Done()
		gotB = make([]byte, len(dataA))
		_, errR1 = io.ReadFull(b2, gotB)
	}()
	go func() {
		defer wg.Done()
		gotA = make([]byte, len(dataB))
		_, errR2 = io.ReadFull(a1, gotA)
	}()
	wg.Wait()
	require.NoError(t, errW1)
	require.NoError(t, errW2)
	require.NoError(t, errR1)
	require.NoError(t, errR2)
	require.True(t, bytes.Equal(dataA, gotB), "a->b payload mismatch")
	require.True(t, bytes.Equal(dataB, gotA), "b->a payload mismatch")

	// All payloads are through; closing one endpoint unwinds the join.
	require.NoError(t, a1.Close())
	select {
	case <-joinDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Join did not terminate after closing an endpoint")
	}
	require.EqualValues(t, len(dataB), inCount, "inCount should be bytes copied c2->c1")
	require.EqualValues(t, len(dataA), outCount, "outCount should be bytes copied c1->c2")
}

func TestJoinAdaptivePath(t *testing.T) {
	testJoinIntegrity(t, true)
}

func TestJoinSpliceFastPath(t *testing.T) {
	testJoinIntegrity(t, false)
}

// scriptedReader returns the scripted byte counts one Read at a time and
// records the buffer size the copy loop offered on each call. A script value
// larger than the offered buffer means "fill the buffer completely".
type scriptedReader struct {
	script   []int
	observed []int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	r.observed = append(r.observed, len(p))
	if len(r.script) == 0 {
		return 0, io.EOF
	}
	n := min(r.script[0], len(p))
	r.script = r.script[1:]
	return n, nil
}

const fillRead = 1 << 30

func TestCopyAdaptiveGrowsAndShrinks(t *testing.T) {
	script := make([]int, 0, 55)
	// 7 buffer-filling reads: two per tier to climb 4k->16k->64k->256k,
	// plus one more full read at the top tier that must not grow further.
	for range 7 {
		script = append(script, fillRead)
	}
	// 48 small reads: 16 per tier to walk back 256k->64k->16k->4k.
	for range 48 {
		script = append(script, 1000)
	}
	src := &scriptedReader{script: script}

	written, err := copyAdaptive(io.Discard, src)
	require.NoError(t, err)

	var want []int
	appendN := func(size, n int) {
		for range n {
			want = append(want, size)
		}
	}
	appendN(4*1024, 2)
	appendN(16*1024, 2)
	appendN(64*1024, 2)
	appendN(256*1024, 1+16) // final full read, then 16 small reads before shrinking
	appendN(64*1024, 16)
	appendN(16*1024, 16)
	appendN(4*1024, 1) // the EOF read happens after shrinking back to the bottom tier
	require.Equal(t, want, src.observed)

	wantWritten := int64(2*4*1024+2*16*1024+2*64*1024+256*1024) + 48*1000
	require.Equal(t, wantWritten, written)
}

func TestCopyAdaptiveNeedsConsecutiveFullReads(t *testing.T) {
	// Alternating full and small reads must never grow the buffer.
	src := &scriptedReader{script: []int{fillRead, 100, fillRead, 100, fillRead}}
	_, err := copyAdaptive(io.Discard, src)
	require.NoError(t, err)
	for i, size := range src.observed {
		require.Equal(t, 4*1024, size, "read %d should still use the bottom tier", i)
	}
}

// joinFixed16k replicates the previous Join implementation (a fixed 16 KiB
// pooled buffer per direction) as a benchmark baseline.
func joinFixed16k(c1 io.ReadWriteCloser, c2 io.ReadWriteCloser) (inCount int64, outCount int64, errs []error) {
	var wait sync.WaitGroup
	recordErrs := make([]error, 2)
	pipe := func(number int, to io.ReadWriteCloser, from io.ReadWriteCloser, count *int64) {
		defer wait.Done()
		defer to.Close()
		defer from.Close()

		buf := pool.GetBuf(16 * 1024)
		defer pool.PutBuf(buf)
		*count, recordErrs[number] = io.CopyBuffer(to, from, buf)
	}

	wait.Add(2)
	go pipe(0, c1, c2, &inCount)
	go pipe(1, c2, c1, &outCount)
	wait.Wait()

	for _, e := range recordErrs {
		if e != nil {
			errs = append(errs, e)
		}
	}
	return
}

func benchmarkJoin(b *testing.B, join func(io.ReadWriteCloser, io.ReadWriteCloser) (int64, int64, []error)) {
	a1, a2 := tcpPair(b)
	b1, b2 := tcpPair(b)
	deadline := time.Now().Add(10 * time.Minute)
	for _, c := range []*net.TCPConn{a1, a2, b1, b2} {
		if err := c.SetDeadline(deadline); err != nil {
			b.Fatal(err)
		}
	}

	go func() {
		_, _, _ = join(wrappedConn{a2}, wrappedConn{b1})
	}()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.Copy(io.Discard, b2)
	}()

	chunk := make([]byte, 1<<20)
	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()
	for range b.N {
		if _, err := a1.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = a1.Close()
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		b.Fatal("drain goroutine did not finish")
	}
}

func BenchmarkJoinAdaptive(b *testing.B) {
	benchmarkJoin(b, Join)
}

func BenchmarkJoinFixed16k(b *testing.B) {
	benchmarkJoin(b, joinFixed16k)
}
