package delayqueue

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// The start of PriorityQueue implementation.
// Borrowed from https://github.com/nsqio/nsq/blob/master/internal/pqueue/pqueue.go

type item struct {
	Value    interface{}
	Priority int64
	Index    int
}

// this is a priority queue as implemented by a min heap
// ie. the 0th element is the *lowest* value
type priorityQueue []*item

func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

func (pq priorityQueue) Len() int {
	return len(pq)
}

// Less < 就是从小到大排序, 所以是一个小顶堆
func (pq priorityQueue) Less(i, j int) bool {
	// 按照优先级来排序
	return pq[i].Priority < pq[j].Priority
}

func (pq priorityQueue) Swap(i, j int) {
	// 交换位置，并交换 index(index就是他的位置)
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 插入一个 item(bucket) 放在最后
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		// 扩容
		npq := make(priorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	// 这种用法还真的是新颖...
	*pq = (*pq)[0 : n+1]
	item := x.(*item)
	item.Index = n
	(*pq)[n] = item
}

// Pop 弹出最后一个 item(bucket)
func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		// 缩容
		npq := make(priorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// PeekAndShift 移除顶点小于 max 的节点(弹出一个bucket)
// 如果顶点的优先级都大于 max 的话, 说明没有节点需要被移除
// priorityQueue 是一个小顶堆
func (pq *priorityQueue) PeekAndShift(max int64) (*item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	// 从小顶堆取最小值, item 里面是一个 bucket
	item := (*pq)[0]
	// 如果最小值还是大于给入的, 那么返回差值
	if item.Priority > max {
		return nil, item.Priority - max
	}
	// 这里就是移除一个节点(把顶点移除掉, 涉及到尾结点和顶点调换位置, 然后调用 down 和 up 进行修复, down 和 up 会调用 Swap 来调节)
	// 因为 priorityQueue 实现了 heap.Interface 接口, 所以其实就是一个 heap, 可以用 heap 的方法来操作
	heap.Remove(pq, 0)

	return item, 0
}

// The end of PriorityQueue implementation.

// DelayQueue is an unbounded blocking queue of *Delayed* elements, in which
// an element can only be taken when its delay has expired. The head of the
// queue is the *Delayed* element whose delay expired furthest in the past.
type DelayQueue struct {
	C chan interface{}

	mu sync.Mutex
	pq priorityQueue

	// Similar to the sleeping state of runtime.timers.
	sleeping int32
	wakeupC  chan struct{}
}

// New creates an instance of delayQueue with the specified size.
func New(size int) *DelayQueue {
	return &DelayQueue{
		C:       make(chan interface{}), // 无缓存 channel 哦
		pq:      newPriorityQueue(size),
		wakeupC: make(chan struct{}), // 无缓存 channel 哦
	}
}

// Offer inserts the element into the current queue.
// 往延时队列插入一条记录(插入一个 bucket)
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, Priority: expiration}

	dq.mu.Lock()
	// 往堆中插入一个数据(bucket), 直接加在最后, 然后调用 up 调整
	// 这里会设置他的 index, 包括 Swap 也是会重置 index 的
	heap.Push(&dq.pq, item)
	index := item.Index
	dq.mu.Unlock()

	// 如果 index == 0 说明是第一个元素
	if index == 0 {
		// A new item with the earliest expiration is added.
		// 如果 dq.sleeping 以前是 1 的话, 设置为 0(表示如果之前是休眠的话, 现在不休眠了), 并往 dq.wakeupC 发送一个 struct{}
		// 简单说就是, 如果以前是 sleeping, 那么就要往 dq.wakeupC 发送一个消息来激活
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			// dq.wakeupC 是无缓存 channel 哦
			dq.wakeupC <- struct{}{}
		}
	}
}

// Poll starts an infinite loop, in which it continually waits for an element
// to expire and then send the expired element to the channel C.
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		// 从优先级队列取一个到期的 bucket, 如果这个元素小于当前时间的话, 则取出小顶堆顶点的元素, 否则返回这个元素过期时间与当前的时间差
		item, delta := dq.pq.PeekAndShift(now)
		// 说明堆是空的或者当前最小元素的过期时间也大于当前时间, 设置休眠状态
		if item == nil {
			// No items left or at least one item is pending.

			// We must ensure the atomicity of the whole operation, which is
			// composed of the above PeekAndShift and the following StoreInt32,
			// to avoid possible race conditions between Offer and Poll.
			// 将 dq.sleeping 设置为 1(表示休眠)
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			// item == nil && delta == 0 说明优先级队列(堆)为空
			if delta == 0 {
				// No items left.
				select {
				// 当有数据插入到优先级队列的时候, 当 index == 0 且 dq.sleeping 为 1 的时候会往 dq.wakeupC 发送一个 struct{}
				case <-dq.wakeupC:
					// Wait until a new item is added.
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 { // item == nil && delta > 0 说明当前时间还没有到期的任务
				// At least one item is pending.
				select {
				case <-dq.wakeupC: // 插入了新的任务, 且触发了执行
					// A new item with an "earlier" expiration than the current "earliest" one is added.
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond): // 启动一个超时时间为到期时间的计时器
					// The current "earliest" item expires.

					// Reset the sleeping state since there's no need to receive from wakeupC.
					// 如果 dq.sleeping 以前就是 0(不休眠) 的话, 死等 dq.wakeupC(此时应该是 Offer 这边往 dq.wakeupC 发了个 struct{})
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						// A caller of Offer() is being blocked on sending to wakeupC,
						// drain wakeupC to unblock the caller.
						// 这里应该就是解决 select 随机 + 并发问题, 可能在计时器到期之前, 有一个任务添加进来,
						// 且到期时间是最低的, 那么就会发送一个 struct{} 到 dq.wakeupC
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		// 到期的直接传给 dq.C
		case dq.C <- item.Value: // bucket
			// The expired element has been sent out successfully.
		case <-exitC:
			goto exit
		}
	}

exit:
	// Reset the states
	atomic.StoreInt32(&dq.sleeping, 0)
}
