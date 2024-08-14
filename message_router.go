package danube

import (
	"sync/atomic"
)

type MessageRouter struct {
	partitions    int32
	lastPartition int32
}

func NewMessageRouter(partitions int32) *MessageRouter {
	return &MessageRouter{
		partitions:    partitions,
		lastPartition: partitions - 1,
	}
}

func (router *MessageRouter) RoundRobin() int32 {
	// Atomically get the current value of lastPartition
	last := atomic.LoadInt32(&router.lastPartition)

	// Calculate the next partition and update atomically
	next := (last + 1) % router.partitions
	atomic.StoreInt32(&router.lastPartition, next)

	return next
}
