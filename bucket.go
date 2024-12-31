package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer represents a single event. When the Timer expires, the given
// task will be executed.
type Timer struct {
	expiration int64 // in milliseconds
	task       func()

	// The bucket that holds the list to which this timer's element belongs.
	//
	// NOTE: This field may be updated and read concurrently,
	// through Timer.Stop() and Bucket.Flush().
	b unsafe.Pointer // type: *bucket

	// The timer's element.
	element *list.Element
}

func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// Stop prevents the Timer from firing. It returns true if the call
// stops the timer, false if the timer has already expired or been stopped.
//
// If the timer t has already expired and the t.task has been started in its own
// goroutine; Stop does not wait for t.task to complete before returning. If the caller
// needs to know whether t.task is completed, it must coordinate with t.task explicitly.
func (t *Timer) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		// If b.Remove is called just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// this may fail to remove t due to the change of t's bucket.
		stopped = b.Remove(t)

		// Thus, here we re-get t's possibly new bucket (nil for case 1, or ab (non-nil) for case 2),
		// and retry until the bucket becomes nil, which indicates that t has finally been removed.
	}
	return stopped
}

type bucket struct {
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we must keep the 64-bit field
	// as the first field of the struct.
	//
	// For more explanations, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// and https://go101.org/article/memory-layout.html.
	// 任务的到期时间(就是啥时候执行)
	expiration int64

	mu sync.Mutex
	// 相同过期时间的任务队列
	timers *list.List
}

func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// Add 直接把 item 加入到链表中
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()

	// 往链表中插入数据, 并放到最后
	e := b.timers.PushBack(t)
	// 把 bucket 引用搞过来
	t.setBucket(b)
	// 把该 Timer 在链表中的 element 搞过来
	t.element = e

	b.mu.Unlock()
}

func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		// If remove is called from t.Stop, and this happens just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// then t.getBucket will return nil for case 1, or ab (non-nil) for case 2.
		// In either case, the returned value does not equal to b.
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// Flush 清空 bucket, 并把 Timer 执行对应的函数
func (b *bucket) Flush(reinsert func(*Timer)) {
	var ts []*Timer

	b.mu.Lock()
	// 循环获取bucket队列节点
	for e := b.timers.Front(); e != nil; {
		next := e.Next()

		t := e.Value.(*Timer)
		// 将头节点移除bucket队列
		b.remove(t)
		ts = append(ts, t)

		e = next
	}
	b.mu.Unlock()

	b.SetExpiration(-1) // TODO: Improve the coordination with b.Add()

	for _, t := range ts {
		// 因为这里面有些可能是上层的任务, 所以这里不能直接执行, 而是尝试重新插入到 timing wheel 中
		reinsert(t)
	}
}
