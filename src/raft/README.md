# 遇到的坑
## 2A
- **RequestVote要并发请求，并发处理，不可等待所有请求返回后在处理(sync.WaitGroup模式)**
- 触发选举后，最多等待electionTimeout随即触发下一次选举
- RequestVote处理收到的请求：若收到之前的过期返回，直接忽略
- reply中的term为接收请求的server的currentTerm 
- matchIndex需与next保持同步
## 2B
- heartbeat只是一种特殊的AppendEntryRPC(entries为空)，无需特殊对待
- leader和follower都应该忽略之前的AppendEntryRPC，否则follower的logs会被错误的覆盖和修改。需要考虑之前的Replica RCP请求(Entries里的LogEntry会重叠)，不能直接更新nextIndex
- `Start`方法中，log replica不应阻塞地等待follower的返回而是直接返回