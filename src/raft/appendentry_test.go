package raft

import (
	"testing"
)

func Test_isOldLogReplica(t *testing.T) {
	type args struct {
		logs    []LogEntry
		entries []LogEntry
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				logs: []LogEntry{
					{
						Index:   1,
						Command: 101,
						Term:    1,
					},
					{
						Index:   2,
						Command: 102,
						Term:    1,
					},
					{
						Index:   3,
						Command: 103,
						Term:    1,
					}, {
						Index:   4,
						Command: 104,
						Term:    1,
					}, {
						Index:   5,
						Command: 104,
						Term:    1,
					},
				},
				entries: []LogEntry{
					{
						Index:   1,
						Command: 101,
						Term:    1,
					},
					{
						Index:   2,
						Command: 103,
						Term:    2,
					},
				},
			},
			want: false,
		},
		{
			args: args{
				logs: []LogEntry{
					{
						Index:   2,
						Command: 102,
						Term:    1,
					},
					{
						Index:   3,
						Command: 103,
						Term:    1,
					}, {
						Index:   4,
						Command: 104,
						Term:    1,
					}, {
						Index:   5,
						Command: 104,
						Term:    1,
					},
				},
				entries: []LogEntry{
					{
						Index:   2,
						Command: 103,
						Term:    2,
					},
					{
						Index:   3,
						Command: 104,
						Term:    2,
					},
				},
			},
			want: false,
		},
		{
			args: args{
				logs: []LogEntry{
					{},
					{
						Index:   1,
						Command: 101,
						Term:    1,
					},
					{
						Index:   2,
						Command: 102,
						Term:    1,
					},
					{
						Index:   3,
						Command: 103,
						Term:    1,
					}, {
						Index:   4,
						Command: 104,
						Term:    1,
					}, {
						Index:   5,
						Command: 104,
						Term:    1,
					},
				},
				entries: []LogEntry{
					{
						Index:   2,
						Command: 102,
						Term:    1,
					},
					{
						Index:   3,
						Command: 103,
						Term:    1,
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOldLogReplica(tt.args.logs, tt.args.entries); got != tt.want {
				t.Errorf("isOldLogReplica() = %v, want %v", got, tt.want)
			}
		})
	}
}
