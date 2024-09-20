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

package raft

import (
	"context"
	"errors"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.trk for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
type RawNode struct {
	raft               *raft
	asyncStorageWrites bool

	// Mutable fields.
	prevSoftSt     *SoftState
	prevHardSt     pb.HardState
	stepsOnAdvance []pb.ContextMessage
	tracer         *tracing.Tracer
}

// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
func NewRawNode(ctx context.Context, config *Config) (*RawNode, error) {
	r := newRaft(ctx, config)
	rn := &RawNode{
		raft:   r,
		tracer: config.Tracer,
	}
	rn.asyncStorageWrites = config.AsyncStorageWrites
	ss := r.softState()
	rn.prevSoftSt = &ss
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick(ctx context.Context) {
	rn.raft.tick(ctx)
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign(ctx context.Context) error {
	return rn.raft.Step(ctx, pb.Message{
		Type: pb.MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(ctx context.Context, data []byte) error {
	log.Event(ctx, "RawNode.Propose")
	return rn.raft.Step(ctx, pb.Message{
		Type: pb.MsgProp,
		From: rn.raft.id,
		Entries: []pb.Entry{
			{Data: data},
		}})
}

// ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
// details.
func (rn *RawNode) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	m, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return rn.raft.Step(ctx, m)
}

// ApplyConfChange applies a config change to the local node. The app must call
// this when it applies a configuration change, except when it decides to reject
// the configuration change, in which case no call must take place.
func (rn *RawNode) ApplyConfChange(ctx context.Context, cc pb.ConfChangeI) *pb.ConfState {
	cs := rn.raft.applyConfChange(ctx, cc.AsV2())
	return &cs
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(ctx context.Context, m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
		return ErrStepLocalMsg
	}
	if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && rn.raft.trk.Progress(m.From) == nil {
		return ErrStepPeerNotFound
	}
	log.Eventf(ctx, "RawNode.Step: %s - index %d, (%d -> %d)", m.Type.String(), m.Index, m.From, m.To)
	return rn.raft.Step(ctx, m)
}

// LogSnapshot returns a point-in-time read-only state of the raft log.
//
// The returned snapshot can be read from while RawNode continues operation, as
// long as the application guarantees immutability of the underlying log storage
// snapshot (returned from the LogStorage.LogSnapshot method) while the snapshot
// is being used.
//
// One way the application can implement an immutable snapshot is by blocking
// the entire log storage for new writes. This also means the Ready() handling
// loop isn't able to hand over log writes to storage.
//
// A more advanced implementation can grab an immutable storage engine snapshot
// that does not block writes. Not blocking writes is beneficial for commit tail
// latency, since it doesn't prevent MsgApp/Resp exchange with the leader.
func (rn *RawNode) LogSnapshot() LogSnapshot {
	return rn.raft.raftLog.snap(rn.raft.raftLog.storage.LogSnapshot())
}

// Ready returns the outstanding work that the application needs to handle. This
// includes appending and applying entries or a snapshot, updating the HardState,
// and sending messages. The returned Ready() *must* be handled and subsequently
// passed back via Advance().
func (rn *RawNode) Ready(ctx context.Context) Ready {
	rd := rn.readyWithoutAccept(ctx)
	rn.acceptReady(ctx, rd)
	return rd
}

// NB: This uses an approach of attaching SOME context to the Ready that has a
// SpanFromContext set. This could be done lazily instead but since we look this
// up multiple times it is better to do it once at creation.
func (rn *RawNode) tracedContext(ctx context.Context, rd Ready) context.Context {
	if tracing.SpanFromContext(ctx) != nil {
		return ctx
	}
	for _, msg := range rd.Messages {
		return msg.Context
	}
	for _, e := range rd.CommittedEntries {
		return rn.raft.lookupContext(ctx, e)
	}
	for _, e := range rd.Entries {
		return rn.raft.lookupContext(ctx, e)
	}
	return ctx
}

// readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
// is no obligation that the Ready must be handled.
func (rn *RawNode) readyWithoutAccept(ctx context.Context) Ready {
	r := rn.raft

	rd := Ready{
		Entries:          r.raftLog.nextUnstableEnts(),
		CommittedEntries: r.raftLog.nextCommittedEnts(ctx, rn.applyUnstableEntries()),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		// Allocate only when SoftState changes.
		escapingSoftSt := softSt
		rd.SoftState = &escapingSoftSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.hasNextUnstableSnapshot() {
		rd.Snapshot = *r.raftLog.nextUnstableSnapshot()
	}
	rd.MustSync = MustSync(r.hardState(), rn.prevHardSt, len(rd.Entries))

	rd.Context = rn.tracedContext(ctx, rd)
	if rn.asyncStorageWrites {
		// If async storage writes are enabled, enqueue messages to
		// local storage threads, where applicable.
		if needStorageAppendMsg(r, rd) {
			m := newStorageAppendMsg(ctx, r, rd)
			rd.Messages = append(rd.Messages, m)
		}
		if needStorageApplyMsg(rd) {
			m := newStorageApplyMsg(r, rd)
			rd.Messages = append(rd.Messages, m)
		}
	} else {
		// If async storage writes are disabled, immediately enqueue
		// msgsAfterAppend to be sent out. The Ready struct contract
		// mandates that Messages cannot be sent until after Entries
		// are written to stable storage.
		for _, m := range r.msgsAfterAppend {
			if m.To != r.id {
				rd.Messages = append(rd.Messages, m)
			}
		}
	}

	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
// NOTE: MustSync isn't used under AsyncStorageWrites mode.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// currentLead
	// currentLeadEpoch
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term ||
		st.Lead != prevst.Lead || st.LeadEpoch != prevst.LeadEpoch || st.Commit != prevst.Commit
}

func needStorageAppendMsg(r *raft, rd Ready) bool {
	// Return true if log entries, hard state, or a snapshot need to be written
	// to stable storage. Also return true if any messages are contingent on all
	// prior MsgStorageAppend being processed.
	return len(rd.Entries) > 0 ||
		!IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) ||
		len(r.msgsAfterAppend) > 0
}

func needStorageAppendRespMsg(rd Ready) bool {
	// Return true if raft needs to hear about stabilized entries or an applied
	// snapshot.
	return !IsEmptySnap(rd.Snapshot) || len(rd.Entries) != 0
}

// newStorageAppendMsg creates the message that should be sent to the local
// append thread to instruct it to append log entries, write an updated hard
// state, and apply a snapshot. The message also carries a set of responses
// that should be delivered after the rest of the message is processed. Used
// with AsyncStorageWrites.
func newStorageAppendMsg(ctx context.Context, r *raft, rd Ready) pb.ContextMessage {
	m := pb.Message{
		Type:    pb.MsgStorageAppend,
		To:      LocalAppendThread,
		From:    r.id,
		Entries: rd.Entries,
	}
	if ln := len(rd.Entries); ln != 0 {
		// See comment in newStorageAppendRespMsg for why the accTerm is attached.
		m.LogTerm = r.raftLog.accTerm()
		m.Index = rd.Entries[ln-1].Index
	}
	if !IsEmptyHardState(rd.HardState) {
		// If the Ready includes a HardState update, assign each of its fields
		// to the corresponding fields in the Message. This allows clients to
		// reconstruct the HardState and save it to stable storage.
		//
		// If the Ready does not include a HardState update, make sure to not
		// assign a value to any of the fields so that a HardState reconstructed
		// from them will be empty (return true from raft.IsEmptyHardState).
		m.Term = rd.Term
		m.Vote = rd.Vote
		m.Commit = rd.Commit
		m.Lead = rd.Lead
		m.LeadEpoch = rd.LeadEpoch
	}
	if !IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot
		m.Snapshot = &snap
		// See comment in newStorageAppendRespMsg for why the accTerm is attached.
		m.LogTerm = r.raftLog.accTerm()
	}
	// Attach all messages in msgsAfterAppend as responses to be delivered after
	// the message is processed, along with a self-directed MsgStorageAppendResp
	// to acknowledge the entry stability.
	//
	// NB: it is important for performance that MsgStorageAppendResp message be
	// handled after self-directed MsgAppResp messages on the leader (which will
	// be contained in msgsAfterAppend). This ordering allows the MsgAppResp
	// handling to use a fast-path in r.raftLog.term() before the newly appended
	// entries are removed from the unstable log.
	var mCtx context.Context
	m.Responses, mCtx = stripContext(r.msgsAfterAppend)
	// Warning: there is code outside raft package depending on the order of
	// Responses, particularly MsgStorageAppendResp being last in this list.
	// Change this with caution.
	if needStorageAppendRespMsg(rd) {
		m.Responses = append(m.Responses, newStorageAppendRespMsg(mCtx, r, rd).Message)
	}
	return pb.NewContextMessage(rd.Context, m)
}

func stripContext(m []pb.ContextMessage) ([]pb.Message, context.Context) {
	responses := make([]pb.Message, len(m))
	var ctx context.Context
	for i, resp := range m {
		responses[i] = resp.Message
		ctx = resp.Context
	}
	return responses, ctx
}

// newStorageAppendRespMsg creates the message that should be returned to node
// after the unstable log entries, hard state, and snapshot in the current Ready
// (along with those in all prior Ready structs) have been saved to stable
// storage.
func newStorageAppendRespMsg(ctx context.Context, r *raft, rd Ready) pb.ContextMessage {
	m := pb.Message{
		Type: pb.MsgStorageAppendResp,
		To:   r.id,
		From: LocalAppendThread,
	}
	if ln := len(rd.Entries); ln != 0 {
		// If sending unstable entries to storage, attach the last index and last
		// accepted term to the response message. This (index, term) tuple will be
		// handed back and consulted when the stability of those log entries is
		// signaled to the unstable. If the term matches the last accepted term by
		// the time the response is received (unstable.stableTo), the unstable log
		// can be truncated up to the given index.
		//
		// The last accepted term logic prevents an ABA problem[^1] that could lead
		// to the unstable log and the stable log getting out of sync temporarily
		// and leading to an inconsistent view. Consider the following example with
		// 5 nodes, A B C D E:
		//
		//  1. A is the leader.
		//  2. A proposes some log entries but only B receives these entries.
		//  3. B gets the Ready and the entries are appended asynchronously.
		//  4. A crashes and C becomes leader after getting a vote from D and E.
		//  5. C proposes some log entries and B receives these entries, overwriting the
		//     previous unstable log entries that are in the process of being appended.
		//     The entries have a larger term than the previous entries but the same
		//     indexes. It begins appending these new entries asynchronously.
		//  6. C crashes and A restarts and becomes leader again after getting the vote
		//     from D and E.
		//  7. B receives the entries from A which are the same as the ones from step 2,
		//     overwriting the previous unstable log entries that are in the process of
		//     being appended from step 5. The entries have the original terms and
		//     indexes from step 2. Recall that log entries retain their original term
		//     numbers when a leader replicates entries from previous terms. It begins
		//     appending these new entries asynchronously.
		//  8. The asynchronous log appends from the first Ready complete and stableTo
		//     is called.
		//  9. However, the log entries from the second Ready are still in the
		//     asynchronous append pipeline and will overwrite (in stable storage) the
		//     entries from the first Ready at some future point. We can't truncate the
		//     unstable log yet or a future read from Storage might see the entries from
		//     step 5 before they have been replaced by the entries from step 7.
		//     Instead, we must wait until we are sure that the entries are stable and
		//     that no in-progress appends might overwrite them before removing entries
		//     from the unstable log.
		//
		// If accTerm has changed by the time the MsgStorageAppendResp is returned,
		// the response is ignored and the unstable log is not truncated. The
		// unstable log is only truncated when the term has remained unchanged from
		// the time that the MsgStorageAppend was sent to the time that the response
		// is received, indicating that no new leader has overwritten the log.
		//
		// TODO(pav-kv): unstable entries can be partially released even if the last
		// accepted term changed, if we track the (term, index) points at which the
		// log was truncated.
		//
		// [^1]: https://en.wikipedia.org/wiki/ABA_problem
		m.LogTerm = r.raftLog.accTerm()
		m.Index = rd.Entries[ln-1].Index
		// TODO: Lookup all contexts until we find one that has tracing info.
		ctx = r.lookupContext(ctx, rd.Entries[ln-1])
	}
	if !IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot
		m.Snapshot = &snap
		m.LogTerm = r.raftLog.accTerm()
	}
	return pb.NewContextMessage(ctx, m)
}

func needStorageApplyMsg(rd Ready) bool     { return len(rd.CommittedEntries) > 0 }
func needStorageApplyRespMsg(rd Ready) bool { return needStorageApplyMsg(rd) }

// newStorageApplyMsg creates the message that should be sent to the local
// apply thread to instruct it to apply committed log entries. The message
// also carries a response that should be delivered after the rest of the
// message is processed. Used with AsyncStorageWrites.
func newStorageApplyMsg(r *raft, rd Ready) pb.ContextMessage {
	ents := rd.CommittedEntries
	return pb.NewContextMessage(
		rd.Context,
		pb.Message{
			Type:    pb.MsgStorageApply,
			To:      LocalApplyThread,
			From:    r.id,
			Term:    0, // committed entries don't apply under a specific term
			Entries: ents,
			Responses: []pb.Message{
				newStorageApplyRespMsg(r, ents),
			},
		},
	)
}

// newStorageApplyRespMsg creates the message that should be returned to node
// after the committed entries in the current Ready (along with those in all
// prior Ready structs) have been applied to the local state machine.
func newStorageApplyRespMsg(r *raft, ents []pb.Entry) pb.Message {
	return pb.Message{
		Type:    pb.MsgStorageApplyResp,
		To:      r.id,
		From:    LocalApplyThread,
		Term:    0, // committed entries don't apply under a specific term
		Entries: ents,
	}
}

// acceptReady is called when the consumer of the RawNode has decided to go
// ahead and handle a Ready. Nothing must alter the state of the RawNode between
// this call and the prior call to Ready().
func (rn *RawNode) acceptReady(ctx context.Context, rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	if !rn.asyncStorageWrites {
		if len(rn.stepsOnAdvance) != 0 {
			log.Fatalf(ctx, "two accepted Ready structs without call to Advance")
		}
		for _, m := range rn.raft.msgsAfterAppend {
			if m.To == rn.raft.id {
				rn.stepsOnAdvance = append(rn.stepsOnAdvance, m)
			}
		}
		if needStorageAppendRespMsg(rd) {
			m := newStorageAppendRespMsg(ctx, rn.raft, rd)
			rn.stepsOnAdvance = append(rn.stepsOnAdvance, m)
		}
		if needStorageApplyRespMsg(rd) {
			// FIXME: Is this the right context?
			m := pb.NewContextMessage(ctx, newStorageApplyRespMsg(rn.raft, rd.CommittedEntries))
			rn.stepsOnAdvance = append(rn.stepsOnAdvance, m)
		}
	}
	rn.raft.msgs = nil
	rn.raft.msgsAfterAppend = nil
	rn.raft.raftLog.acceptUnstable()
	if len(rd.CommittedEntries) > 0 {
		ents := rd.CommittedEntries
		index := ents[len(ents)-1].Index
		rn.raft.raftLog.acceptApplying(ctx, index, entsSize(ents), rn.applyUnstableEntries())
	}
}

// applyUnstableEntries returns whether entries are allowed to be applied once
// they are known to be committed but before they have been written locally to
// stable storage.
func (rn *RawNode) applyUnstableEntries() bool {
	return !rn.asyncStorageWrites
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// TODO(nvanbenschoten): order these cases in terms of cost and frequency.
	r := rn.raft
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if r.raftLog.hasNextUnstableSnapshot() {
		return true
	}
	if len(r.msgs) > 0 || len(r.msgsAfterAppend) > 0 {
		return true
	}
	if r.raftLog.hasNextUnstableEnts() || r.raftLog.hasNextCommittedEnts(rn.applyUnstableEntries()) {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
//
// NOTE: Advance must not be called when using AsyncStorageWrites. Response messages from
// the local append and apply threads take its place.
func (rn *RawNode) Advance(ctx context.Context, _ Ready) {
	// The actions performed by this function are encoded into stepsOnAdvance in
	// acceptReady. In earlier versions of this library, they were computed from
	// the provided Ready struct. Retain the unused parameter for compatibility.
	if rn.asyncStorageWrites {
		log.Fatalf(ctx, "Advance must not be called when using AsyncStorageWrites")
	}
	for i, m := range rn.stepsOnAdvance {
		_ = rn.raft.Step(m.Context, m.Message)
		rn.stepsOnAdvance[i] = pb.ContextMessage{}
	}
	rn.stepsOnAdvance = rn.stepsOnAdvance[:0]
}

// Term returns the current in-memory term of this RawNode. This term may not
// yet have been persisted in storage.
func (rn *RawNode) Term() uint64 {
	return rn.raft.Term
}

// Lead returns the leader of Term(), or None if the leader is unknown.
func (rn *RawNode) Lead() pb.PeerID {
	return rn.raft.lead
}

// LogMark returns the current log mark of the raft log. It is not guaranteed to
// be in stable storage, unless this method is called right after RawNode is
// initialized (in which case its state reflects the stable storage).
func (rn *RawNode) LogMark() LogMark {
	return rn.raft.raftLog.unstable.mark()
}

// NextUnstableIndex returns the index of the next entry that will be sent to
// local storage, if there are any. All entries < this index are either stored,
// or have been sent to storage.
//
// NB: NextUnstableIndex can regress when the node accepts appends or snapshots
// from a newer leader.
func (rn *RawNode) NextUnstableIndex() uint64 {
	return rn.raft.raftLog.unstable.entryInProgress + 1
}

// SendPing sends a MsgApp ping to the given peer, if it is in StateReplicate
// and there was no recent MsgApp to this peer.
//
// Returns true if the ping was added to the message queue.
func (rn *RawNode) SendPing(ctx context.Context, to pb.PeerID) bool {
	return rn.raft.sendPing(ctx, to)
}

// Status returns the current status of the given group. This allocates, see
// SparseStatus, BasicStatus and WithProgress for allocation-friendlier choices.
func (rn *RawNode) Status() Status {
	status := getStatus(rn.raft)
	return status
}

// BasicStatus returns a BasicStatus. Notably this does not contain the
// Progress map; see WithProgress for an allocation-free way to inspect it.
func (rn *RawNode) BasicStatus() BasicStatus {
	return getBasicStatus(rn.raft)
}

// SparseStatus returns a SparseStatus. Notably, it doesn't include Config and
// Progress.Inflights, which are expensive to copy.
func (rn *RawNode) SparseStatus() SparseStatus {
	return getSparseStatus(rn.raft)
}

// LeadSupportStatus returns a LeadSupportStatus. Notably, it only includes
// leader support information.
func (rn *RawNode) LeadSupportStatus() LeadSupportStatus {
	return getLeadSupportStatus(rn.raft)
}

// ProgressType indicates the type of replica a Progress corresponds to.
type ProgressType byte

const (
	// ProgressTypePeer accompanies a Progress for a regular peer replica.
	ProgressTypePeer ProgressType = iota
	// ProgressTypeLearner accompanies a Progress for a learner replica.
	ProgressTypeLearner
)

// WithProgress is a helper to introspect the Progress for this node and its
// peers.
func (rn *RawNode) WithProgress(visitor func(id pb.PeerID, typ ProgressType, pr tracker.Progress)) {
	withProgress(rn.raft, visitor)
}

// ReportUnreachable reports the given node is not reachable for the last send.
func (rn *RawNode) ReportUnreachable(ctx context.Context, id pb.PeerID) {
	_ = rn.raft.Step(ctx, pb.Message{Type: pb.MsgUnreachable, From: id})
}

// ReportSnapshot reports the status of the sent snapshot.
func (rn *RawNode) ReportSnapshot(ctx context.Context, id pb.PeerID, status SnapshotStatus) {
	rej := status == SnapshotFailure

	_ = rn.raft.Step(ctx, pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej})
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(ctx context.Context, transferee pb.PeerID) {
	_ = rn.raft.Step(ctx, pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

// ForgetLeader forgets a follower's current leader, changing it to None.
// See (Node).ForgetLeader for details.
func (rn *RawNode) ForgetLeader(ctx context.Context) error {
	return rn.raft.Step(ctx, pb.Message{Type: pb.MsgForgetLeader})
}

func (rn *RawNode) TestingStepDown() error {
	return rn.raft.testingStepDown(context.Background())
}

func (rn *RawNode) TestingFortificationStateString() string {
	return rn.raft.fortificationTracker.String()
}
