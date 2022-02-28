package raft

import (
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	timer := time.NewTimer(time.Second * 10)
	go func() {
		ti := time.Now()
		for {
			select {
			case <-timer.C:
				t.Logf("timer after %s", time.Now().Sub(ti))
			}
		}
	}()
	for {

	}
}
