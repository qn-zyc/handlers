package handlers

import (
	"container/list"
	"sync"
)

// Handlers 的状态
const (
	StatusInit    int32 = iota // 初始化中
	StatusRunning              // 正在运行
	StatusStop                 // 已停止
)

// Source 数据源
type Source interface {
	Next() (data interface{}, err error)
}

// Handler 处理器
type Handler interface {
	// ID 表示Handler的唯一标识
	ID() string
	// Handle 处理输入数据，返回的数据将用于下一个处理器。
	Handle(in interface{}) (dataForNextHandler interface{})
	// Result 获取结果, 或者自己处理结果。
	Result() interface{}
}

type safeList struct {
	sync.RWMutex
	*list.List
}

// Handlers 处理器集合。
type Handlers struct {
	sync.RWMutex
	todoSrc       *safeList // 未处理源
	doneSrc       *safeList // 已处理源
	handlers      *safeList // 处理链
	resultHandler Handler   // 结果处理器
	state         int32     // Handlers的状态
}

// AddSrc 添加待处理的数据源
func (h *Handlers) AddSrc(src Source) {
	if h.todoSrc == nil {
		h.Lock()
		if h.todoSrc == nil {
			h.todoSrc = new(safeList)
		}
		h.Unlock()
	}
	h.todoSrc.Lock()
	h.todoSrc.PushBack(src)
	h.todoSrc.Unlock()
}

// popSrc 获取一个待处理源。
func (h *Handlers) popSrc() Source {
	if h.todoSrc == nil {
		return nil
	}
	h.todoSrc.Lock()
	ele := h.todoSrc.Front()
	h.todoSrc.Remove(ele)
	h.todoSrc.Unlock()
	return ele.Value.(Source)
}

// srcDone src已经处理完毕。
func (h *Handlers) srcDone(src Source) {
	if h.doneSrc == nil {
		h.Lock()
		if h.doneSrc == nil {
			h.doneSrc = new(safeList)
		}
		h.Unlock()
	}
	h.doneSrc.Lock()
	h.doneSrc.PushBack(src)
	h.doneSrc.Unlock()
}

// AddHandler 添加处理器。
func (h *Handlers) AddHandler(handler Handler) {
	if h.handlers == nil {
		h.Lock()
		if h.handlers == nil {
			h.handlers = new(safeList)
		}
		h.Unlock()
	}
	h.handlers.Lock()
	h.handlers.PushBack(handler)
	h.handlers.Unlock()
}

// SetResultHandler 设置结果处理器
func (h *Handlers) SetResultHandler(handler Handler) {
	h.Lock()
	h.resultHandler = handler
	h.Unlock()
}

// Run 执行。
func (h *Handlers) Run() {
	// 防止多次调用Run().
	// 初始化状态和停止状态都可以再次调用Run().
	h.Lock()
	if h.state == StatusRunning {
		h.Unlock()
		return
	}
	h.state = StatusRunning
	h.Unlock()

	for {
		src := h.popSrc()
		if src == nil {
			break
		}
		h.handleSrc(src)
	}

	// 处理结果
	if h.resultHandler != nil && h.handlers != nil {
		results := make(map[string]interface{})
		h.handlers.RLock()
		defer h.handlers.RUnlock()
		for e := h.handlers.Front(); e != nil; e = e.Next() {
			handler := e.Value.(Handler)
			results[handler.ID()] = handler.Result()
		}
		h.resultHandler.Handle(results)
	}
}

func (h *Handlers) handleSrc(src Source) {
	if h.handlers == nil {
		return
	}
	h.handlers.RLock()
	defer h.handlers.RUnlock()
	for d, err := src.Next(); err == nil; d, err = src.Next() {
		for e := h.handlers.Front(); e != nil; e = e.Next() {
			d = e.Value.(Handler).Handle(d)
		}
	}
}
