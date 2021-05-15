// https://raft.github.io/raft.pdf
// a simple implementation of raft

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

// 节点结构体，用于存储每个节点的地址信息
type node struct {
	address string
}

// State 定义
type State int

// raft 节点三种状态：Follower、Candidate、Leadder
// 仅用于区分
const (
	Follower  State = iota + 1 // 1
	Candidate                  // 2
	Leader                     // 3
)

type LogEntry struct {
	LogTerm  int
	LogIndex int
	LogCMD   interface{}
}

// Raft Node 结构体定义，对应论文的State
type RaftNode struct {
	// 当前节点index
	me int
	// 除去当前节点外其他节点信息
	nodes map[int]*node
	// 当前节点状态State
	state State

	// 以下定义每个server都需要持久化的状态：
	// 当前任期
	currentTerm int
	// 当前任期投票给了谁，未投票设置为-1
	votedFor int
	// 当前任期获取的投票数量，超过半数由candidate成为follower
	voteCount int
	// 日志条目集合
	log []LogEntry

	// 以下定义每个server不需要持久化的状态：
	// 被提交的最大索引
	commitIndex int
	// 被应用到状态机的最大索引
	// 这里直接append log而没有进一步使用log中的命令
	// 请参见5.3节或Rules for Servers->All Servers第一条规则
	// apply log[lastApplied] to state machine
	// lastApplied int

	// 以下定义leader的不需要持久化的状态（将会在选主时从1开始重新初始化）
	// 保存需要发给每个节点的下一个条目索引
	nextIndex []int
	// 保存已经复制给每个节点日志的最高索引
	matchIndex []int

	// heartbeat channel
	heartbeatC chan bool
	// to Leader channel
	// 一段时间后收到半数以上投票, toLeaderC会被设为true，此时由Candidate转变为Leader
	toLeaderC chan bool
}

// 给定地址，新建一个节点
func newNode(address string) *node {
	node := &node{}
	node.address = address
	return node
}

// 对应论文的RequestVote RPC
// Invoked by candidates to gather votes
// 声明RaftNode类的方法
// 即RequestVote rpc 方法
func (rf *RaftNode) RequestVote(args VoteArgs, reply *VoteReply) error {
	// 投票请求任期小于自己的任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// 没有投过票且投票请求任期大于等于自己的任期，投票给它，并将自己的任期更新为投票请求的任期
	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	return nil
}

// arguments for request vote rpc
// 对应RequestVote RPC的arguments
// 其中LastLogIndex和LastLogTerm没有用到
type VoteArgs struct {
	Term        int
	CandidateID int
	// LastLogIIndex int
	// LastLogTerm   int
}

// 对应RequestVote RPC 的Results
type VoteReply struct {
	// 当前任期号，以便候选人去更新自己的任期号
	Term int
	// 候选人赢得此张选票时为真
	VoteGranted bool
}

func (rf *RaftNode) broadcastRequestVote() {
	var args = VoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := range rf.nodes {
		go func(i int) {
			var reply VoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

func (rf *RaftNode) sendRequestVote(serverID int, args VoteArgs, reply *VoteReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing: ", err)
	}

	defer client.Close()
	// 调用client的RequestVote进行vote
	client.Call("Raft.RequestVote", args, reply)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
	}

	// 如果投票超过半数，成为leader
	if rf.voteCount >= len(rf.nodes)/2+1 {
		rf.toLeaderC <- true
	}

}

// 对应论文的AppendEntries RPC参数
type AppendEntriesArgs struct {
	// Leader 任期
	Term int
	// Leader的ID，这样Follower可以把请求重定向给Leader
	LeaderID int

	// 新日志之前的索引
	PrevLogIndex int
	// PrevLogIndex的任期号
	PrevLogTerm int
	// 准备存储的日志条目
	Entries []LogEntry
	// Leader 已经commit 的索引值
	LeaderCommit int
}

// 对应论文的AppendEntries RPC
// invoked by leader to replicate log entries, also used as heartbeat
// AppendEntries rpc 方法
func (rf *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	// 如果leader节点小于当前节点term，失败
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果只是heartbeat, 没有append entries, 就告诉自己的任期
	// 对应论文upon election: send initial empty AppendEntries RPCs(heartbeat)
	// to each server, repeat during idle periods to prevent election timeouts
	rf.heartbeatC <- true
	if len(args.Entries) == 0 {
		reply.Success = true
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果有append entries，但leader维护的LogIndex大于当前Follower的LogIndex
	// 代表当前Follower失联过, 所以Follower要告知Leader它当前
	// 的最大索引，以便下次心跳Leader返回
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return nil
	}

	// append Entries not in log
	// args.Entries的元素被打散一个个append进入rf.log
	rf.log = append(rf.log, args.Entries...)

	rf.commitIndex = rf.getLastIndex()
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = Min(args.LeaderCommit, rf.getLastIndex())
	// }

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = rf.getLastIndex() + 1

	return nil

}

type AppendEntriesReply struct {
	Success bool
	Term    int

	// 如果Follower Index小于Leader Index，会告诉Leader下次开始发送的索引位置
	// leader 下一次会返回Follower这NextIndex之后的log
	NextIndex int
}

func (rf *RaftNode) broadcastAppendEntries() {
	for i := range rf.nodes {
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.LeaderCommit = rf.commitIndex

		// 计算preLogIndex, preLogTerm
		// 提取preLogIndex - baseIndex之后的entries，发生给follower
		prevLogIndex := rf.nextIndex[i] - 1
		if rf.getLastIndex() > prevLogIndex {
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.log[prevLogIndex].LogTerm
			args.Entries = rf.log[prevLogIndex:]
			log.Printf("send entries: %v\n", args.Entries)
		}

		go func(i int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			rf.sendAppendEntries(i, args, &reply)
		}(i, args)
	}
}

func (rf *RaftNode) sendAppendEntries(serverID int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()
	client.Call("Raft.AppendEntries", args, reply)

	// 对应If successful: update nextIndex and matchIndex for follower
	if reply.Success {
		if reply.NextIndex > 0 {
			rf.nextIndex[serverID] = reply.NextIndex
			rf.matchIndex[serverID] = rf.nextIndex[serverID] - 1
		}
	} else {
		// 如果leader的term小于follower的term，需要将leader转变成follower重新选举
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}
	}

}

func (rf *RaftNode) rpc(port string) {
	rpc.Register(rf)
	rpc.HandleHTTP()
	go func() {
		err := http.ListenAndServe(port, nil)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
	}()
}

// 对应论文Rules for Servers
// 对于Followers, Candidates和Leaders三种情况的规则
func (rf *RaftNode) start() {
	// 初始状态Follower，任期0，没有votedFor
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	go func() {
		rand.Seed(time.Now().UnixNano())

		for {
			switch rf.state {
			case Follower:
				select {
				case <-rf.heartbeatC:
					log.Printf("follower-%d received heartbeat\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
					log.Printf("follower-%d timeout\n", rf.me)
					rf.state = Candidate
				}
			case Candidate:
				fmt.Printf("Node: %d, I'm candidate\n", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				go rf.broadcastRequestVote()

				// case 1: 一段时间后没有收到半数以上投票，成为不了leader
				// case 2: 一段时间后收到半数以上投票, toLeaderC会被设为true，此时由Candidate转变为Leader
				select {

				case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
					rf.state = Follower
				case <-rf.toLeaderC:
					fmt.Printf("Node: %d, I am leader\n", rf.me)
					rf.state = Leader
					// 初始化follower的nextIndex和matchIndex
					rf.nextIndex = make([]int, len(rf.nodes))
					rf.matchIndex = make([]int, len(rf.nodes))
					for i := range rf.nodes {
						rf.nextIndex[i] = 1
						rf.matchIndex[i] = 0
					}

					// 每隔3秒添加一个log
					go func() {
						i := 0
						for {
							i++
							rf.log = append(rf.log, LogEntry{rf.currentTerm, i, fmt.Sprintf("user send: %d", i)})
							time.Sleep(3 * time.Second)
						}
					}()
				}
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(100 * time.Millisecond)

			}
		}
	}()

}

func (rf *RaftNode) getLastIndex() int {
	rlen := len(rf.log)
	if rlen == 0 {
		return 0
	}
	return rf.log[rlen-1].LogIndex
}

// 没有用到
// func (rf *RaftNode) getLastTerm() int {
// 	rlen := len(rf.log)
// 	if rlen == 0 {
// 		return 0
// 	}
// 	return rf.log[rlen-1].LogTerm
// }

// func Min(x, y int) int {
// 	if x < y {
// 		return x
// 	}
// 	return y
// }

func main() {
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	flag.Parse()
	clusters := strings.Split(*cluster, ",")

	ns := make(map[int]*node)
	for k, v := range clusters {
		ns[k] = newNode(v)
	}

	raft := &RaftNode{}
	raft.me = *id
	raft.nodes = ns
	raft.rpc(*port)
	raft.start()

	select {}

}
