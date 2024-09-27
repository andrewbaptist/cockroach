// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
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

package rafttest

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/rafttype"
)

// a network interface
type iface interface {
	send(m rafttype.Message)
	recv() chan rafttype.Message
	disconnect()
	connect()
}

type raftNetwork struct {
	rand         *rand.Rand
	mu           sync.Mutex
	disconnected map[rafttype.PeerID]bool
	dropmap      map[conn]float64
	delaymap     map[conn]delay
	recvQueues   map[rafttype.PeerID]chan rafttype.Message
}

type conn struct {
	from, to rafttype.PeerID
}

type delay struct {
	d    time.Duration
	rate float64
}

func newRaftNetwork(nodes ...rafttype.PeerID) *raftNetwork {
	pn := &raftNetwork{
		rand:         rand.New(rand.NewSource(1)),
		recvQueues:   make(map[rafttype.PeerID]chan rafttype.Message),
		dropmap:      make(map[conn]float64),
		delaymap:     make(map[conn]delay),
		disconnected: make(map[rafttype.PeerID]bool),
	}

	for _, n := range nodes {
		pn.recvQueues[n] = make(chan rafttype.Message, 1024)
	}
	return pn
}

func (rn *raftNetwork) nodeNetwork(id rafttype.PeerID) iface {
	return &nodeNetwork{id: id, raftNetwork: rn}
}

func (rn *raftNetwork) send(m rafttype.Message) {
	rn.mu.Lock()
	to := rn.recvQueues[m.To]
	if rn.disconnected[m.To] {
		to = nil
	}
	drop := rn.dropmap[conn{m.From, m.To}]
	dl := rn.delaymap[conn{m.From, m.To}]
	rn.mu.Unlock()

	if to == nil {
		return
	}
	if drop != 0 && rn.rand.Float64() < drop {
		return
	}
	// TODO: shall we dl without blocking the send call?
	if dl.d != 0 && rn.rand.Float64() < dl.rate {
		rd := rn.rand.Int63n(int64(dl.d))
		time.Sleep(time.Duration(rd))
	}

	// use marshal/unmarshal to copy message to avoid data race.
	b, err := m.Marshal()
	if err != nil {
		panic(err)
	}

	var cm rafttype.Message
	err = cm.Unmarshal(b)
	if err != nil {
		panic(err)
	}

	select {
	case to <- cm:
	default:
		// drop messages when the receiver queue is full.
	}
}

func (rn *raftNetwork) recvFrom(from rafttype.PeerID) chan rafttype.Message {
	rn.mu.Lock()
	fromc := rn.recvQueues[from]
	if rn.disconnected[from] {
		fromc = nil
	}
	rn.mu.Unlock()

	return fromc
}

func (rn *raftNetwork) drop(from, to rafttype.PeerID, rate float64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.dropmap[conn{from, to}] = rate
}

func (rn *raftNetwork) delay(from, to rafttype.PeerID, d time.Duration, rate float64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.delaymap[conn{from, to}] = delay{d, rate}
}

func (rn *raftNetwork) disconnect(id rafttype.PeerID) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.disconnected[id] = true
}

func (rn *raftNetwork) connect(id rafttype.PeerID) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.disconnected[id] = false
}

type nodeNetwork struct {
	id rafttype.PeerID
	*raftNetwork
}

func (nt *nodeNetwork) connect() {
	nt.raftNetwork.connect(nt.id)
}

func (nt *nodeNetwork) disconnect() {
	nt.raftNetwork.disconnect(nt.id)
}

func (nt *nodeNetwork) send(m rafttype.Message) {
	nt.raftNetwork.send(m)
}

func (nt *nodeNetwork) recv() chan rafttype.Message {
	return nt.recvFrom(nt.id)
}
