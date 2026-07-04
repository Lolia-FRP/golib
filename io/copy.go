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
	"errors"
	"io"
	"net"
	"sync"
)

// bufSizes are the copy buffer tiers used by Join. Each tier is 4x the
// previous one, so a read that uses at most a quarter of the current buffer
// is exactly a read that would have fit in the tier below.
var bufSizes = []int{4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024}

const (
	// growAfterFullReads is how many consecutive reads must completely fill
	// the current buffer before stepping up one tier.
	growAfterFullReads = 2
	// shrinkAfterSmallReads is how many consecutive reads fitting in the tier
	// below (at most a quarter of the current buffer) before stepping down.
	shrinkAfterSmallReads = 16
)

// bufPools holds one pool per tier. Pointers are pooled instead of slices to
// avoid an allocation on every Put (staticcheck SA6002).
var bufPools = func() []*sync.Pool {
	pools := make([]*sync.Pool, len(bufSizes))
	for i, size := range bufSizes {
		pools[i] = &sync.Pool{
			New: func() any {
				buf := make([]byte, size)
				return &buf
			},
		}
	}
	return pools
}()

var errInvalidWrite = errors.New("invalid write result")

func copyConn(dst io.Writer, src io.Reader) (int64, error) {
	// Between two raw TCP/Unix connections io.Copy relays in-kernel via
	// splice(2), which beats any userspace buffer.
	if spliceCapable(dst) && spliceCapable(src) {
		return io.Copy(dst, src)
	}
	return copyAdaptive(dst, src)
}

func spliceCapable(v any) bool {
	switch v.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	default:
		return false
	}
}

// copyAdaptive copies from src to dst like io.Copy, resizing the copy buffer
// between reads based on how much of it recent reads used. Buffers grow while
// a connection sustains bulk transfers and shrink back once traffic turns
// small, so idle or interactive connections pin little memory while
// high-throughput streams get large reads and fewer syscalls.
//
// It deliberately does not delegate to io.CopyBuffer: that would take the
// WriterTo/ReaderFrom fast paths whenever one endpoint is a raw *net.TCPConn,
// and with a wrapped connection on the other side those paths degrade to a
// fixed 32 KiB stdlib buffer allocated per connection, defeating adaptive
// sizing. The genuinely useful kernel fast path (both endpoints raw) is
// handled by copyConn before this function is reached.
func copyAdaptive(dst io.Writer, src io.Reader) (written int64, err error) {
	tier := 0
	bufPtr := bufPools[tier].Get().(*[]byte)
	defer func() {
		bufPools[tier].Put(bufPtr)
	}()

	fullReads, smallReads := 0, 0
	for {
		buf := *bufPtr
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errInvalidWrite
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}

			switch {
			case nr == len(buf):
				smallReads = 0
				fullReads++
				if fullReads >= growAfterFullReads && tier < len(bufPools)-1 {
					bufPools[tier].Put(bufPtr)
					tier++
					bufPtr = bufPools[tier].Get().(*[]byte)
					fullReads = 0
				}
			case tier > 0 && nr*4 <= len(buf):
				fullReads = 0
				smallReads++
				if smallReads >= shrinkAfterSmallReads {
					bufPools[tier].Put(bufPtr)
					tier--
					bufPtr = bufPools[tier].Get().(*[]byte)
					smallReads = 0
				}
			default:
				fullReads, smallReads = 0, 0
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
