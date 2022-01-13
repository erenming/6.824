# Leader Election
## 遇到的坑
- **RequestVote要并发请求，并发处理，不可等待所有请求返回后在处理(sync.WaitGroup模式)**
- 触发选举后，最多等待electionTimeout随即触发下一次选举
- RequestVote处理收到的请求：若收到之前的过期返回，直接忽略 
