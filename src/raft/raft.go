package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

type ServerState int

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

const (
	HeartbeatTime      = time.Duration(100) * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 2 * ElectionTimeoutMin
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//Added Struct Entry
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//End Added
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	state      ServerState

	electionTimer *time.Timer

	candidateToFollower chan int

	leaderToFollower chan int

	heartbeat chan int

	commit chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//AppendEntries RPC arguments structure
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PervLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//AppendEntries RPC reply structure
type AppendEntryReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	var toFollower = (args.Term > rf.currentTerm)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	switch rf.state {
	case LEADER:
		if toFollower {
			rf.leaderToFollower <- 1
		} else {
			return
		}
		break
	case CANDIDATE:
		if toFollower {
			rf.candidateToFollower <- 1
		}
		break

	}

	lastLog := rf.getLogEntry(len(rf.logs) - 1)
	if (args.LastLogTerm < lastLog.Term) || args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index {
		rf.persist()
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.handleTimeout()
	}
}

/**
* Get log Entry by index.
 */

func (rf *Raft) getLogEntry(index int) LogEntry {
	return rf.logs[index]
}

/*
* AppendEntries RPC handler.
 */
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {

	rf.mu.Lock()
	reply.Success = false
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.logs[len(rf.logs)-1].Index + 1
		return
	}

	var toFollower = (args.LeaderId != rf.me && args.Term > rf.currentTerm)
	switch rf.state {
	case FOLLOWER:
		break
	case CANDIDATE:
		if toFollower {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.candidateToFollower <- 1
			rf.persist()
		}
		break
	case LEADER:
		if toFollower {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.leaderToFollower <- 1
			rf.persist()
		}
		break
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.handleTimeout()

	reply.Term = rf.currentTerm

	lastLogEntry := rf.getLogEntry(len(rf.logs) - 1)

	if args.PervLogIndex > lastLogEntry.Index {
		reply.NextIndex = lastLogEntry.Index + 1
		return
	}

	firstLogEntry := rf.getLogEntry(0)
	logEntry := rf.getLogEntry(args.PervLogIndex - firstLogEntry.Index)

	if args.PervLogIndex > firstLogEntry.Index && args.PrevLogTerm != logEntry.Term {
		for i := firstLogEntry.Index; i <= args.PervLogIndex-1; i++ {
			if rf.logs[i-firstLogEntry.Index].Term != logEntry.Term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	if args.PervLogIndex >= firstLogEntry.Index {
		rf.logs = rf.logs[:args.PervLogIndex+1-firstLogEntry.Index]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
		reply.Success = true
		reply.NextIndex = rf.logs[len(rf.logs)-1].Index
	}

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLogEntry(len(rf.logs) - 1)
		if args.LeaderCommit > last.Index {
			rf.commitIndex = last.Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commit <- 1

	}
}

func (rf *Raft) handleTimeout() {
	num := rand.Intn(int(ElectionTimeoutMax - ElectionTimeoutMin))
	i := int(ElectionTimeoutMin) + num
	rf.electionTimer.Reset(time.Duration(i) * time.Millisecond)
}

/*
* Handle follower
 */

func (rf *Raft) handleFollower() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.currentTerm += 1
			rf.state = CANDIDATE
			rf.votedFor = -1
			rf.handleTimeout()
			rf.persist()
			return
		}
	}

}

/*
* Handle candidate
 */
func (rf *Raft) handleCandidate() {
	// Vote to self
	rf.votedFor = rf.me
	rf.persist()

	grantedVoteCount := 1

	becomeLeader := make(chan int)
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term}
	for server := 0; server < len(rf.peers); server++ {
		if rf.me == server {
			continue
		}

		go func(s int, args RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(s, args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.candidateToFollower <- 1
				} else if reply.VoteGranted {
					grantedVoteCount++
					if grantedVoteCount >= len(rf.peers)/2+1 {
						becomeLeader <- 1
					}
				}
			}
		}(server, args)

	}

	select {
	case <-becomeLeader:
		rf.doLeader()
		return
	case <-rf.electionTimer.C:
		rf.handleTimeout()
		rf.currentTerm += 1
		rf.votedFor = -1
		rf.persist()
		return
	case <-rf.candidateToFollower:
		rf.state = FOLLOWER
		return
	}

}

func (rf *Raft) doLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = LEADER
	rf.votedFor = -1
	rf.persist()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
		rf.matchIndex[i] = 0
	}

}

func (rf *Raft) initRaftIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = rf.lastApplied + 1
		rf.matchIndex[server] = 0
	}
	rf.persist()
}

/*
* Handle candidate
 */
func (rf *Raft) handleLeader() {

	rf.initRaftIndex()

	// Heartbeat timer
	heartbeatTimer := time.NewTicker(HeartbeatTime)
	defer heartbeatTimer.Stop()

	go func() {
		for range heartbeatTimer.C {
			rf.heartbeat <- 1
		}
	}()

	for {
		select {
		case <-rf.leaderToFollower:
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return

		case <-rf.heartbeat:
			commitIndex := rf.commitIndex
			baseIndex := rf.logs[0].Index
			lastIndex := rf.logs[len(rf.logs)-1].Index
			for i := rf.commitIndex + 1; i <= lastIndex; i++ {
				count := 1
				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}

					if rf.matchIndex[j] >= i && rf.logs[i-baseIndex].Term == rf.currentTerm {
						count++
					}
				}
				if len(rf.peers) < 2*count {
					commitIndex = i
				}
			}

			if commitIndex != rf.commitIndex {
				rf.commitIndex = commitIndex
				rf.commit <- 1

			}

			rf.broadcastAppendEntries()
		}
	}

}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prervLogIndex := rf.nextIndex[i] - 1
		if prervLogIndex > len(rf.logs)-1 {
			prervLogIndex = len(rf.logs) - 1
		}
		args := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PervLogIndex: prervLogIndex,
			PrevLogTerm:  rf.logs[prervLogIndex].Term,
			LeaderCommit: rf.commitIndex,
		}

		baseIndex := rf.logs[0].Index
		if rf.nextIndex[i] > baseIndex {
			args.Entries = make([]LogEntry, len(rf.logs[args.PervLogIndex+1:]))
			copy(args.Entries, rf.logs[args.PervLogIndex+1:])
		}

		var reply AppendEntryReply

		go func(i int, args AppendEntryArgs) {
			ok := rf.sendAppendEntries(i, args, &reply)
			if ok {
				if reply.Success {
					if len(args.Entries) > 0 {
						rf.mu.Lock()
						rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						rf.mu.Unlock()
					}
				} else {
					rf.nextIndex[i] = reply.NextIndex
				}
			}

		}(i, args)
	}
}

func (rf *Raft) launch(applyCh chan ApplyMsg) {

	go func() {
		for {
			select {
			case <-rf.commit:
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex && i < len(rf.logs); i++ {
					msg := ApplyMsg{Index: i, Command: rf.logs[i].Command}
					applyCh <- msg
					rf.lastApplied = i

				}
			}
		}
	}()

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				rf.handleFollower()
				break
			case CANDIDATE:
				rf.handleCandidate()
				break
			case LEADER:
				rf.handleLeader()
				break
			}
		}
	}()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state == LEADER {
		isLeader = true
		index = rf.logs[len(rf.logs)-1].Index + 1
		logEntry := LogEntry{
			index,
			term,
			command,
		}
		rf.logs = append(rf.logs, logEntry)
		rf.persist()
		rf.heartbeat <- 1
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = FOLLOWER

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.electionTimer = time.NewTimer(2 * ElectionTimeoutMin * time.Millisecond)

	rf.candidateToFollower = make(chan int)
	rf.leaderToFollower = make(chan int)

	rf.heartbeat = make(chan int)
	rf.commit = make(chan int)

	rf.handleTimeout()
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize the state
	rf.readPersist(persister.ReadRaftState())

	// end initialize the state

	rf.launch(applyCh)

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	return rf
}
