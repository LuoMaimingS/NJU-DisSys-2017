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
import "labrpc"
import "time"
import "bytes"
import "encoding/gob"
import "math/rand"

// server state
type ServerState string

const (
	FOLLOWER				= "Follower"
	CANDIDATE				= "Candidate"
	LEADER					= "Leader"
)

const (
	HEARTBEAT_TIME			= 150
	ELECTION_MIN_TIME		= 200
	ELECTION_MAX_TIME		= 400
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

type LogEntry struct {
	Idx	int
	Term	int
	Command	interface{}
}



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

	// Election state
	currentTerm	int
	votedFor	int

	// Log state
	logs		[]LogEntry
	commitIdx	int
	lastApplied	int

	// Leader state
	nextIdx		[]int
	matchIdx	[]int

	state		ServerState

	electionTimer *time.Timer

	applyCh	chan ApplyMsg

	grantedVoteCount	int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

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
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
	}
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         	int
	CandidateId  	int
	LastLogIdx		int
	LastLogTerm  	int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int
	VoteGranted		bool
}


//AppendEntries RPC
type AppendEntriesArgs struct{
	Term			int
	LeaderId		int
	PrevLogIdx		int
	PrevLogTerm		int
	Logs 			[]LogEntry
	LeaderCommit 	int
}

//AppendEntries RPC
type AppendEntriesReply struct{
	Term 			int
	Success			bool
	CommitIdx		int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// referrence: https://github.com/bysui/mit6.824/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	grantedVote := true

	logSize := len(rf.logs)
	if logSize > 0 {
		if rf.logs[logSize - 1].Term > args.LastLogTerm {
			grantedVote = false
		} else if rf.logs[logSize - 1].Term ==  args.LastLogTerm && logSize - 1 > args.LastLogIdx {
			grantedVote = false
		}
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && grantedVote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if grantedVote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()

		reply.Term = args.Term
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else { //args.Term < rf.currentTerm 
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

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
	index := -1
	term := -1
	isLeader := true


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

func (rf *Raft) resetTimer() {
	
	randst := ELECTION_MIN_TIME + rand.Int63n(ELECTION_MAX_TIME - ELECTION_MIN_TIME)
	timeout :=  time.Millisecond * time.Duration(randst)
	if rf.state == LEADER {
		randst = HEARTBEAT_TIME
		timeout = time.Millisecond * HEARTBEAT_TIME
	}
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeout)
		go func() {
			for {
				<- rf.electionTimer.C
				rf.timerHandler()
			}
		}()
	}
	rf.electionTimer.Reset(timeout)


}

func (rf *Raft) timerHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		rf.BroadcastAppendEntries()
	} else {
		rf.state = CANDIDATE
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		rf.grantedVoteCount = 1
		rf.persist()

		logSize := len(rf.logs)
		args := RequestVoteArgs {
			Term:			rf.currentTerm,
			CandidateId:	rf.me,
			LastLogIdx:		logSize - 1,
		}

		if logSize > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIdx].Term
		}

		for s := 0; s < len(rf.peers); s++ {
			if s != rf.me {
				go func(s int, args RequestVoteArgs) {
					var reply RequestVoteReply
					if rf.sendRequestVote(s, args, &reply) {
						rf.countGrandtedVotes(reply)
					}
				}(s, args)
			}
		}
	}
	rf.resetTimer()
}


func (rf *Raft) countGrandtedVotes(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		return
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	} else {
		if rf.state == CANDIDATE && reply.VoteGranted {
			rf.grantedVoteCount += 1
			if rf.grantedVoteCount >= len(rf.peers) / 2 + 1 {
				rf.state = LEADER
			}

			for s := 0; s < len(rf.peers); s++ {
				if s != rf.me {
					rf.nextIdx[s] = len(rf.logs)
					rf.matchIdx[s] = -1
				}
			}

			rf.resetTimer()
		}
		return
	}
}


func (rf *Raft) BroadcastAppendEntries() {
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			args := AppendEntriesArgs {
				Term:		rf.currentTerm,
				LeaderId:	rf.me,
				PrevLogIdx:	rf.nextIdx[server] - 1,
			}

			if args.PrevLogIdx >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIdx].Term
			}

			if rf.nextIdx[server] < len(rf.logs) {
				args.Logs = rf.logs[rf.nextIdx[server]:]
			}

			args.LeaderCommit = rf.commitIdx

			go func(s int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				if rf.peers[s].Call("Raft.AppendEntries", args, &reply) {
					rf.handleAppendEntries(s, reply)
				}
			}(server, args)
		}
	}
}


func (rf *Raft) handleAppendEntries(server int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.resetTimer()

		} else {
			if reply.Success {
				rf.nextIdx[server] = reply.CommitIdx + 1
				rf.matchIdx[server] = reply.CommitIdx
				num := 1
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIdx[i] >= rf.matchIdx[server] {
						num += 1
					}
				}

				if num >= len(rf.peers) / 2 + 1 &&
					rf.commitIdx < rf.matchIdx[server] &&
					rf.logs[rf.matchIdx[server]].Term == rf.currentTerm {
						rf.commitIdx = rf.matchIdx[server]
						go rf.logCommit()
				}

			} else {
				rf.nextIdx[server] = reply.CommitIdx + 1
				rf.BroadcastAppendEntries()
			}
		}

	}
}


func (rf *Raft) logCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIdx > len(rf.logs) - 1 {
		rf.commitIdx = len(rf.logs) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIdx; i++ {
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIdx
}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term
		if args.PrevLogIdx >= 0 &&
			(len(rf.logs)-1 < args.PrevLogIdx || rf.logs[args.PrevLogIdx].Term != args.PrevLogTerm) {
			reply.CommitIdx = len(rf.logs) - 1
			if reply.CommitIdx > args.PrevLogIdx {
				reply.CommitIdx = args.PrevLogIdx
			}
			for reply.CommitIdx >= 0 {
				if rf.logs[reply.CommitIdx].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIdx--
			}
			reply.Success = false
		} else if args.Logs != nil {
			rf.logs = rf.logs[:args.PrevLogIdx+1]
			rf.logs = append(rf.logs, args.Logs...)
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIdx = args.LeaderCommit
				go rf.logCommit()
			}
			reply.CommitIdx = len(rf.logs) - 1
			reply.Success = true
		} else {
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIdx = args.LeaderCommit
				go rf.logCommit()
			}
			reply.CommitIdx = args.PrevLogIdx
			reply.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIdx = -1
	rf.lastApplied = -1

	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	rf.resetTimer()

	return rf
}
