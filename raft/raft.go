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
	"errors"
	"math/rand"
	"sort"
	
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		votes:            make(map[uint64]bool),
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	// Initialize Raft state from persistent storage.
	hardState, snapshotState, _ := r.RaftLog.storage.InitialState()
	r.Term = hardState.GetTerm()
	r.Vote = hardState.GetVote()
	lastLogIndex := r.RaftLog.LastIndex()

    // Initialize progress trackers for each peer.
    var peers []uint64
    if c.peers == nil {
        peers = snapshotState.Nodes
    } else {
        peers = c.peers
    }
	for _, peerID := range peers {
		if peerID == r.id {
			r.Prs[peerID] = &Progress{Next: lastLogIndex + 1, Match: lastLogIndex} // For self, match is current last index
		} else {
			r.Prs[peerID] = &Progress{Next: lastLogIndex + 1}                    // For others, initialize next to one after last index
		}
	}


	// Set a randomized election timeout.
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// Start as a follower.
	r.becomeFollower(r.Term, None)

    // Set the applied index if provided.
    if c.Applied > 0 {
        r.RaftLog.applied = c.Applied
    }

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
    prevLogTerm, err := r.RaftLog.Term(prevLogIndex)

    if err == ErrCompacted {
        r.sendSnapshot(to) // If log is compacted, send snapshot instead
        return false
    } else if err != nil {
        panic(err) // Handle other errors
    }

    var entriesToSend []*pb.Entry
	// Calculate the starting index for entries to send.
    startIndex := prevLogIndex - r.RaftLog.LastAppend + 1

	// Iterate through the available log entries starting from `startIndex` and append to entriesToSend
    for i := startIndex; i < uint64(len(r.RaftLog.entries)); i++ {
		entriesToSend = append(entriesToSend, &r.RaftLog.entries[i])
	}


	// Create and append the append message to the outbound message queue.
    r.msgs = append(r.msgs, pb.Message{
        MsgType: pb.MessageType_MsgAppend,
        From:    r.id,
        To:      to,
        Term:    r.Term,
        Commit:  r.RaftLog.committed,
        LogTerm: prevLogTerm,
        Index:   prevLogIndex,
        Entries: entriesToSend,
    })
	return true
}
func (r *Raft) sendSnapshot(to uint64) {
	//log.Infof("end snapshot2...")
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendHeartBeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to, LastLogIndex, LastLogTerm uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: LastLogTerm,
		Index:   LastLogIndex,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendEntriesResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.followerTick()
	case StateCandidate:
		r.candidateTick()
	case StateLeader:
		r.leaderTick()
	default:
		//panic("unknown raft node state")
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		//log.Info("heartbeat time out")
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}
func (r *Raft) candidateTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		_ = r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}
func (r *Raft) followerTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		_ = r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.resetElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = 0
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
    r.Lead = r.id
    r.Vote = r.id

    // Initialize progress for all peers.
    for peerID := range r.Prs {
        r.Prs[peerID] = &Progress{
            Next:  r.RaftLog.LastIndex() + 1, // Set next index to one beyond the last log entry
            Match: 0,                        // Initially, no log entries are matched.
        }
    }
    // Leader's own progress is set to match its last log index.
    r.Prs[r.id].Match = r.RaftLog.LastIndex()

    // Create a no-op entry to commit in the new term.
    noopEntry := &pb.Entry{
        Term:  r.Term,
        Index: r.RaftLog.LastIndex() + 1, // Index is immediately after the last log index.
    }

	// Construct a propose message to append the no-op entry to the log.
    proposeMessage := pb.Message{
        From:    r.id,
        To:      r.id,
        MsgType: pb.MessageType_MsgPropose,
        Entries: []*pb.Entry{noopEntry}, // Wrap entry in slice
    }

    // Step the raft using this propose message
    _ = r.Step(proposeMessage)
}

func (r *Raft) resetElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.followerStep(m)
	case StateCandidate:
		r.candidateStep(m)
	case StateLeader:
		r.leaderStep(m)
	default:
		return errors.New("unknown raft node state")
	}
	return nil
}

func (r *Raft) followerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	default:
	}
}

func (r *Raft) candidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	default:
	}
}

func (r *Raft) leaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		//log.Info("heart beat")
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) handleStartElection(m pb.Message) {
	//log.Infof("%d starts election\n", r.id)
	//log.Infof("raft id: %d, term: %d, state: %d; msg: %v, msg term:%d",r.id, r.Term, r.State, m, m.Term)
	 r.becomeCandidate()
    r.resetHeartbeatTimer() 
    
    if r.isSingleNodeCluster() {
        r.becomeLeader()
        return
    }

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	
    for peerID := range r.Prs {
        if peerID != r.id {
            r.sendRequestVoteToPeer(peerID, lastLogIndex, lastLogTerm) 
        }
    }
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//log.Infof("raft id: %d, term: %d, state: %d; msg: %v, msg term:%d",r.id, r.Term, r.State, m, m.Term)
	if m.Term < r.Term {
        // 如果收到的投票响应的任期小于当前任期，则忽略
		return
	}

    // 记录来自节点的投票
	r.votes[m.From] = !m.Reject

	grantedVotesCount := 0
    totalVoters := len(r.votes)
    majorityThreshold := len(r.Prs) / 2
	
	for _, voteGranted := range r.votes {
		if voteGranted {
			grantedVotesCount++
		}
	}

	if grantedVotesCount > majorityThreshold {
        // 如果赢得大多数选票，则成为领导者
		r.becomeLeader()
	} else if totalVoters-grantedVotesCount > majorityThreshold {
        // 如果被大多数节点拒绝，则回到追随者状态
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) resetHeartbeatTimer() {
	r.heartbeatElapsed = 0
}

func (r *Raft) isSingleNodeCluster() bool {
	return len(r.Prs) == 1
}

func (r *Raft) sendRequestVoteToPeer(peerID uint64, lastLogIndex uint64, lastLogTerm uint64) {
    r.sendRequestVote(peerID, lastLogIndex, lastLogTerm)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//log.Infof("raft id: %d, term: %d, state: %d; msg: %v, msg term:%d",r.id, r.Term, r.State, m, m.Term)
	// 如果请求投票的任期小于当前任期，则拒绝投票
    if m.Term < r.Term && m.Term != None {
		r.sendRejectVoteResponse(m.From)
		return
	}
	
	// 如果请求投票的任期大于当前任期，则转换为追随者
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

    // 如果已经投票给其他候选人，则拒绝投票
	if r.Vote != None && r.Vote != m.From {
		r.sendRejectVoteResponse(m.From)
		return
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		//log.Infof("Error getting last log term: %v", err) // 明确日志信息
	}

    // 如果本地日志比请求投票的候选人新，则拒绝投票
	if lastLogTerm > m.LogTerm || (lastLogTerm == m.LogTerm && lastLogIndex > m.Index) {
		r.sendRejectVoteResponse(m.From)
		return
	}

    // 接受投票请求，记录投票信息
	r.Vote = m.From
       r.resetElectionTimer()
	r.electionElapsed = 0
	r.sendAcceptVoteResponse(m.From)
}

func (r *Raft) handleBeat(m pb.Message) {
	for peerID := range r.Prs {
        if peerID != r.id {
            //log.Info("Sending heartbeat to peer") // 更清晰的日志信息
            r.sendHeartbeat(peerID)
        }
    }
}

func (r *Raft) resetElectionTimer() {
    r.electionElapsed = 0
}


func (r *Raft) sendRejectVoteResponse(to uint64) {
	r.sendRequestVoteResponse(to, true)
}

func (r *Raft) sendAcceptVoteResponse(to uint64) {
    r.sendRequestVoteResponse(to, false)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None &&m.Term < r.Term {
		r.sendAppendEntriesResponse(m.From, true, None, None)
		return
	}
	r.becomeFollower(m.Term, m.From)
	firstLogIndex, _ := r.RaftLog.storage.FirstIndex()
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendEntriesResponse(m.From, true, None, r.RaftLog.LastIndex()+1)
		return
	}
	if m.Index >= firstLogIndex {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil {
			return
		} else if term != m.LogTerm {
			var index uint64
			for i := m.Index - r.RaftLog.LastAppend; i >= 0; i-- {
				if r.RaftLog.entries[i].Term == term {
					index = i + r.RaftLog.LastAppend
					break
				}
			}
			r.sendAppendEntriesResponse(m.From, true, term, index)
			return
		}
	}

	for i, e := range m.Entries {
		if e.Index < r.RaftLog.LastAppend {
			continue
		}
		if e.Index <= r.RaftLog.LastIndex() {
			Term, err := r.RaftLog.Term(e.Index)
			if err != nil {
				panic(err)
			}
			if Term != e.Term {
				idx := e.Index - r.RaftLog.LastAppend
				r.RaftLog.entries[idx] = *e
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, e.Index-1)
			}
		} else {
			for j := i; j < len(m.Entries); j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendEntriesResponse(m.From, false, None, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	//log.Infof("raft id: %d, term: %d, state: %d; msg: %v, msg term:%d",r.id, r.Term, r.State, m, m.Term)

	if m.Term < r.Term {
		return // Ignore responses from past terms
	}

	if m.Reject {
		if m.Term > r.Term {
            // If the response has a higher term, become a follower
			r.becomeFollower(m.Term, 0)
		} else {
            // If the response was rejected with the same term, it must be a log inconsistency
            // Decrement the follower's `Next` index and retry an append
			r.Prs[m.From].Next = m.Index 
			r.sendAppend(m.From) // Retry AppendEntries with updated `Next`
		}
	} else {
        // If the response was accepted
        // Update the `Match` and `Next` for the given follower
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

        // Find the median match index
		matches := make(uint64Slice, 0, len(r.Prs))
		for _, p := range r.Prs {
            if p != nil {
			    matches = append(matches, p.Match)
            }
		}

		// If there is only one server, the commit index is the next entry to send
		if len(matches) == 1 {
			r.RaftLog.committed = r.Prs[r.id].Next - 1
			return
		}


        sort.Sort(matches)
        mid := matches[(len(matches)-1)/2] // Calculate median

		// Check if the median index is higher than current commit index and that the term
		// at the median index is the current term
		if mid > r.RaftLog.committed {
			logTerm, err := r.RaftLog.Term(mid)
			if err != nil {
				panic(err) // This should not happen if the log is consistent
			} else if logTerm == r.Term {
				// Commit the entries up to the median index
				r.RaftLog.committed = mid
                // Update leader's next and match
				r.Prs[r.id].Next = r.Prs[r.id].Match + 1
				r.Prs[r.id].Match = max(r.RaftLog.committed, r.Prs[r.id].Match)

                // Send append entries to all other followers in case they haven't gotten it yet
				for p := range r.Prs {
					if p != r.id && r.Prs[p] != nil {
						r.sendAppend(p)
					}
				}
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None &&m.Term < r.Term {
		r.sendHeartBeatResponse(m.From, true)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartBeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	r.sendAppend(m.From)
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, e := range m.Entries {
		e.Index = r.RaftLog.LastIndex() + 1
		e.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snapshotMeta := m.Snapshot.Metadata

    // Reject the snapshot if the term is outdated
	if m.Term < r.Term {
		r.sendAppendEntriesResponse(m.From, true, None, r.RaftLog.committed)
		return
	}

    // Ignore the snapshot if the snapshot index is not greater than the committed index
	if m.Term == r.Term && snapshotMeta.Index <= r.RaftLog.committed {
		r.sendAppendEntriesResponse(m.From, true, None, r.RaftLog.committed)
		return
	}

    // Become follower with the latest term and leader ID
	r.becomeFollower(m.Term, m.From)

    // Extract the snapshot index
	snapshotIndex := snapshotMeta.Index

    // Reset the raft log entries
	r.RaftLog.entries = nil

    // Set the next append index to one after the snapshot index
	r.RaftLog.LastAppend = snapshotIndex + 1

    // Store the incoming snapshot
	r.RaftLog.pendingSnapshot = m.Snapshot

    // Update committed, applied, and stabled indices to the snapshot index
    r.RaftLog.committed = snapshotIndex
    r.RaftLog.applied = snapshotIndex
    r.RaftLog.stabled = snapshotIndex

    // Reinitialize progress for peers based on the snapshot's configuration
    r.Prs = make(map[uint64]*Progress)
    for _, nodeID := range snapshotMeta.ConfState.Nodes {
        r.Prs[nodeID] = &Progress{
			Next: r.RaftLog.LastIndex() + 1, 
			Match: r.RaftLog.LastIndex(),
		}
    }

    // Acknowledge the snapshot with an append entries response
	r.sendAppendEntriesResponse(m.From, false, m.Term, r.RaftLog.LastIndex())
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
