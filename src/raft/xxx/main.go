package main

import (
	"fmt"
	"sync"
	"time"
)

var sharedRsc = false

func main() {
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	go func() {
	begin:
		// this go routine wait for changes to the sharedRsc
		c.L.Lock()
		for !sharedRsc {
			c.Wait()
		}
		fmt.Println("goroutine1")
		c.L.Unlock()
		goto begin
	}()

	go func() {
		// this go routine wait for changes to the sharedRsc
		c.L.Lock()
		for !sharedRsc {
			c.Wait()
		}
		fmt.Println("goroutine2")
		c.L.Unlock()
	}()

	// this one writes changes to sharedRsc
	for {
		c.L.Lock()
		sharedRsc =true
		c.Broadcast()
		c.L.Unlock()
		time.Sleep(5*time.Second)
		c.L.Lock()
		sharedRsc =false
		c.L.Unlock()
	}

}
