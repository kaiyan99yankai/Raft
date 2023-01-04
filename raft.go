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

import (
	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//gloab variable
const (
	heartBeatTime  time.Duration = 180 * time.Millisecond
	followerState                = 0
	candidateState               = 1
	leaderState                  = 2
)

//get a random time duration between 200 t0 400 milliseconds
func random_election_time() time.Duration {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return time.Duration(r1.Intn(200)+200) * time.Millisecond
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//log entry
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	Empty            bool //if the entry is just a heartbeat
	Dissmiss         bool
	ConflictingIndex int
	ConflictingTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//for election
	grantedVote int

	//basci condition
	state int
	//for log appended
	applyCh chan ApplyMsg

	//persistent state
	//the following needed to be presistent in case of crash
	currentTerm int
	votedFor    int
	logs        []*Entry

	//volatile state
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  []int //the index of the next log entry to be sent to corresponding to the peers
	matchIndex []int //the index of the highest log entry known to be applied

	//two timer to start the election or send heartBeat by the leader
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == leaderState
	//fmt.Printf("Start checking {Node %v} with {term %v}\n", rf.me, rf.currentTerm)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	fmt.Printf("(Node %v)'s state change, need persist\n", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Printf("Error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//check if the first element is at least up-to-date to the second

//check if the candidate's log is at least up to date to the raft
func (rf *Raft) is_up_to_date(args *RequestVoteArgs) bool {
	logTerm, logIndex := rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
	//fmt.Printf("(Node %v) with term %v and (Node %v) with term %v\n", args.CandidateId, args.LastLogTerm, rf.me, logTerm)
	if args.LastLogTerm > logTerm {
		return true
	} else if args.LastLogTerm == logTerm && args.LastLogIndex >= logIndex {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	//divided to three case
	//the args.term cpmpared with the rf.currentTerm
	if args.Term < rf.currentTerm {
		//would simply dismiss the request and sent back the currentTerm for the requester to increase
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			//if same term and already grant the vote to other machine
			reply.Term, reply.VoteGranted = rf.currentTerm, false
		} else {
			//check if the log is up to date
			reply.Term = args.Term
			reply.VoteGranted = rf.is_up_to_date(args)
			if reply.VoteGranted {
				fmt.Printf("(Node %v) get vote from (Node %v) with {term %v}\n", args.CandidateId, rf.me, args.Term)
				rf.votedFor = args.CandidateId
				if !rf.electionTimer.Stop() {
					<-rf.electionTimer.C
				}
				rf.electionTimer.Reset(random_election_time())
			}
		}
	} else {
		//grant vote anyway
		//fmt.Printf("(Node %v) get vote from (Node %v) with {term %v}\n", args.CandidateId, rf.me, args.Term)
		reply.Term = args.Term
		rf.state = followerState
		rf.grantedVote = 0
		rf.currentTerm = args.Term
		reply.VoteGranted = rf.is_up_to_date(args)
		if reply.VoteGranted {
			fmt.Printf("(Node %v) get vote from (Node %v) with {term %v}\n", args.CandidateId, rf.me, args.Term)
			rf.votedFor = args.CandidateId
		}
		rf.electionTimer.Reset(random_election_time())
	}
	rf.mu.Unlock()

	//here I did not change the state of the raft server if the candidate's term is
	//larger than the raft server
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.state == leaderState
	if isLeader {
		//make an new entry, append the log and replicate it
		fmt.Printf("Append a log to the leader (Node %v)\n", rf.me)
		entry := new(Entry)
		entry.Index, entry.Term, entry.Command = index, term, command
		rf.logs = append(rf.logs, entry)
		rf.persist()
		//fmt.Printf("Now the length of the leader's log is %v\n", len(rf.logs))
		rf.nextIndex[rf.me] = len(rf.logs)
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		//broadcast to append the entry
		rf.broadCastHeartbeat(false)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//cuurent we only consider if the term is at least as large as

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("try (Node %v) of state %v\n", rf.me, rf.state)
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state != leaderState) {
		//fmt.Printf("check if there is log conflict of (Node %v)\n", rf.me)
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.grantedVote = 0
			rf.currentTerm = args.Term
			rf.persist()
		}
		rf.state = followerState
		//if len(args.Entries) > 0 {
		//fmt.Printf("%v, %v\n", len(rf.logs), args.PrevLogIndex)

		if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			//no conflict found
			fmt.Printf("No conflict found in (Node %v)\n", rf.me)

			if args.Term == rf.logs[len(rf.logs)-1].Term && len(args.Entries)+args.PrevLogIndex < len(rf.logs) {
				fmt.Printf("Already updated\n")
				reply.Dissmiss = true
				reply.Success = false
			} else {
				reply.Success = true
				if len(rf.logs) > args.PrevLogIndex+1 {
					fmt.Printf("log of (Node %v) is cut until pre-index %v\n", rf.me, args.PrevLogIndex)
					rf.logs = rf.logs[:args.PrevLogIndex+1]
					rf.persist()
					fmt.Printf("(Node %v) is now %v\n", rf.me, len(rf.logs))
				}
				//delete all the log after pre-index and append the log if not empty
				if len(args.Entries) > 0 {
					//append the log if the entry is not empty
					rf.logs = append(rf.logs, args.Entries...)
					rf.persist()
					fmt.Printf("(Node %v) appended the log and log length is increased to %v\n", rf.me, len(rf.logs))
				} else {
					reply.Empty = true
				}
			}

			if args.LeaderCommit > rf.commitIndex {
				if len(rf.logs)-1 > args.LeaderCommit {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.logs) - 1
				}
				//fmt.Printf("Try replicate\n")
				if rf.lastApplied+1 <= rf.commitIndex {
					rf.replicate(rf.lastApplied+1, rf.commitIndex)
				}
			}
		} else {
			fmt.Printf("%v, %v\n", len(rf.logs), args.PrevLogIndex)
			if len(rf.logs) > args.PrevLogIndex {
				fmt.Printf("%v, %v\n", rf.logs[args.PrevLogIndex].Term, args.PrevLogIndex)
				reply.ConflictingTerm = rf.logs[args.PrevLogIndex].Term
				i := args.PrevLogIndex
				for ; i >= 0; i-- {
					if rf.logs[i].Term != reply.ConflictingTerm {
						break
					}
				}
				reply.ConflictingIndex = i + 1
			} else {
				reply.ConflictingIndex = len(rf.logs)
			}

			fmt.Printf("there is conflict for (Node %v)\n", rf.me)
			reply.Success = false
		}
		//}
		//fmt.Printf("%v, %v", args.LeaderCommit, rf.commitIndex)

		//fmt.Printf("(Node %v)'s election time has been reset\n", rf.me)
		rf.electionTimer.Reset(random_election_time())
	} else {
		//the leader has smaller term than the followers
		//simply dismiss the coming rpc
		reply.Success = false
	}
	reply.Term = rf.currentTerm

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) tryAppendEntry(peer int) {
	args := new(AppendEntriesArgs)
	rf.mu.RLock()
	args.Term = rf.currentTerm
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = rf.logs[args.PrevLogIndex+1:]
	rf.mu.RUnlock()
	reply := new(AppendEntriesReply)
	fmt.Printf("(Node %v) try to replicate logs with (Node %v) for entry of length %v with pre-index %v\n", rf.me, peer, len(args.Entries), args.PrevLogIndex)
	if rf.sendAppendEntries(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm || rf.state != leaderState {
			//out of date appendentry
			return
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.grantedVote = 0
				rf.state = followerState
				rf.votedFor = -1
				rf.persist()
				fmt.Printf("(Node %v) comes back to follower\n", rf.me)
				rf.electionTimer.Reset(random_election_time())
			} else {
				//fmt.Printf("(Node %v) with same term\n", peer)
				//the follower with same term
				if reply.Success {
					//succeed in replicate the log
					//only when the reply success and the index has been updated, we can start next round broadcast
					rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					if reply.Empty {
						//just heartbeat
						return
					}
					fmt.Printf("(Node %v) agree to replicate the log until %v\n", peer, rf.matchIndex[peer])
					//fmt.Printf("(Node %v) with next index %v\n", peer, rf.nextIndex[peer])
					//also check the matchIndex within this term
					go func() {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.matchIndex[peer] > rf.commitIndex && rf.logs[rf.matchIndex[peer]].Term == rf.currentTerm {
							commitedCount := 0
							for _, value := range rf.matchIndex {
								if value > rf.commitIndex {
									commitedCount += 1
								}
							}
							if commitedCount > len(rf.peers)/2 {
								rf.commitIndex = rf.matchIndex[peer]
								fmt.Printf("Leader (Node %v)'s commitIndex increase to %v\n", rf.me, rf.commitIndex)
								//replicate all
								if rf.lastApplied+1 <= rf.commitIndex {
									rf.replicate(rf.lastApplied+1, rf.commitIndex)
								}
								// rf.broadCastHeartbeat(true)
								// rf.heartBeatTimer.Reset(heartBeatTime)
							}
						}
					}()
				} else {
					if reply.Dissmiss {
						return
					}
					rf.nextIndex[peer] = reply.ConflictingIndex
					fmt.Printf("Decrease (Node %v)'s nextIndex to %v\n", peer, rf.nextIndex[peer])
					//time.Sleep(50 * time.Millisecond)
					go rf.tryAppendEntry(peer)
				}
			}
		}

	}
}
func (rf *Raft) broadCastHeartbeat(isEmpty bool) {
	//if is Empty is true, send empty rpc as heartbeat
	//else replicate log
	if isEmpty {
		fmt.Printf("Just a heartbeat\n")
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.tryAppendEntry(peer)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.grantedVote = 1
			rf.currentTerm += 1
			rf.state = candidateState
			rf.votedFor = rf.me
			rf.persist()
			fmt.Printf("{Node %v} starts election with (term %v)\n", rf.me, rf.currentTerm)
			args := new(RequestVoteArgs)
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.logs[len(rf.logs)-1].Index
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			for peer := range rf.peers {
				if peer == rf.me {
					continue
				}
				go func(peer int) {
					reply := new(RequestVoteReply)
					if rf.sendRequestVote(peer, args, reply) {
						//fmt.Printf("(Node %v) receivce vote\n", rf.me)
						//receive from the peer
						rf.mu.Lock()
						defer rf.mu.Unlock()
						//fmt.Printf("Term: %v, %v; state: %v\n", rf.currentTerm, args.Term, rf.state)
						if rf.currentTerm == args.Term && rf.state == candidateState {
							//term did not change and the state is still candidate
							if reply.VoteGranted {
								//did get the vote
								//fmt.Printf("{Node %v} gets vote\n", rf.me)
								rf.grantedVote += 1
								if rf.grantedVote > len(rf.peers)/2 {
									//elected to be the new leader
									rf.state = leaderState
									rf.electionTimer.Stop()
									//initialize the nextIndex as the len of the log
									for i := range rf.nextIndex {
										rf.nextIndex[i] = len(rf.logs)
										rf.matchIndex[i] = 0
									}
									fmt.Printf("{Node %v} becomes leader\n", rf.me)
									//rf.broadCastHeartbeat(true)
									rf.broadCastHeartbeat(true)
									rf.heartBeatTimer.Reset(heartBeatTime)
									//set a goroutine to
								}
							} else {
								if reply.Term > rf.currentTerm {
									//fmt.Printf("{Node %v} becomes follower\n", rf.me)
									rf.currentTerm = reply.Term
									rf.grantedVote = 0
									rf.state = followerState
									rf.votedFor = -1
									rf.persist()
									rf.electionTimer.Reset(random_election_time())
								}
							}
						}
					}
				}(peer)
			}
			rf.electionTimer.Reset(random_election_time())
			rf.mu.Unlock()
			//fmt.Printf("Unlock\n")
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == leaderState {
				//broadcast heartbeat
				fmt.Printf("(Node %v) broadcast heartbeat\n", rf.me)
				rf.broadCastHeartbeat(true)
				rf.heartBeatTimer.Reset(heartBeatTime)
			}
			rf.mu.Unlock()
		}

	}
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

func (rf *Raft) replicate(startIndex int, endIndex int) {

	//replicate from the startIndex of the log
	if rf.killed() {
		return
	}
	fmt.Printf("(Node %v) start replicate from %v to %v\n", rf.me, startIndex, endIndex)
	//fmt.Printf("(Node %v) has sent log from %v to rsm\n", rf.me, startIndex)
	for _, value := range rf.logs[startIndex : endIndex+1] {

		msg := &ApplyMsg{
			Command:      value.Command,
			CommandIndex: value.Index,
			CommandValid: true,
		}
		rf.applyCh <- *msg
	}
	fmt.Printf("(Node %v) apply successfully\n", rf.me)
	rf.lastApplied = endIndex
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//set the value of the initial raft
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0, //one bit decided if the raft is killed

		grantedVote: 0,

		state: followerState,

		applyCh: applyCh,

		//persistent state
		//the following needed to be presistent in case of crash
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]*Entry, 1),
		//still need to define the log entry

		//volatile state
		commitIndex: 0,
		lastApplied: 0,

		//volatile state on leader
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		//timer
		heartBeatTimer: time.NewTimer(heartBeatTime),
		electionTimer:  time.NewTimer(random_election_time()),
	}
	//fmt.Printf("{Node %v} has been made\n", me)
	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	emptyLog := new(Entry)
	emptyLog.Term, emptyLog.Index = 0, 0
	rf.logs[0] = emptyLog
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
