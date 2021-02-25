package module

import (
	"sync"
	"time"
)

type TaskQueue struct {
	lock     sync.Mutex
	elements []*Service
	count    int
}

func NewTaskQueue() TaskQueue {
	return TaskQueue{
		elements: make([]*Service, 0),
	}
}

func (q *TaskQueue) Empty() bool {
	q.lock.Lock()
	empty := q.count == 0
	q.lock.Unlock()

	return empty
}

func (q *TaskQueue) Put(element *Service) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.elements = append(q.elements, element)
	q.count++
}

func (q *TaskQueue) Take() (*Service, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.count == 0 {
		return nil, false
	}
	var result *Service
	for i := 0; i < len(q.elements); i++ {
		window := q.elements[i].Job.WindowPeriod
		//没有窗口期 移除元素
		if window.StartHour == 0 && window.EndHour == 0 || (time.Now().Hour() >= window.StartHour && time.Now().Hour() <= window.EndHour) {
			result = q.elements[i]
			q.elements = append(q.elements[:i], q.elements[i+1:]...)
			q.count--
			break
		}
	}
	return result, result != nil
}

func (q *TaskQueue) RemoveByID(id string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	for i := 0; i < len(q.elements); i++ {
		if q.elements[i].Job.ID == id {
			q.elements = append(q.elements[:i], q.elements[i+1:]...)
			q.count--
			return true
		}
	}
	return false
}
