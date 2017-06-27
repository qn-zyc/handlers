package handlers

import (
	"container/list"
	"errors"
	"io"
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
	// Handle 处理输入数据，返回的数据将用于下一个处理器。
	Handle(in interface{}) (dataForNextHandler interface{}, err error)
}

// HandlerFunc function式Handler.
type HandlerFunc func(in interface{}) (interface{}, error)

// Handle 实现Handler接口。
func (hf HandlerFunc) Handle(in interface{}) (interface{}, error) { return hf(in) }

type safeList struct {
	sync.RWMutex
	*list.List
}

func newSafeList() *safeList {
	return &safeList{
		List: list.New(),
	}
}

// Handlers 处理器集合。
type Handlers struct {
	sync.RWMutex
	todoSrc  *safeList // 未处理源
	doneSrc  *safeList // 已处理源
	handlers *safeList // 处理链
	state    int32     // Handlers的状态
	ErrCheck func(err error) (goon bool)
}

// AddSrc 添加待处理的数据源
func (h *Handlers) AddSrc(src Source) {
	if h.todoSrc == nil {
		h.Lock()
		if h.todoSrc == nil {
			h.todoSrc = newSafeList()
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
			h.doneSrc = newSafeList()
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
			h.handlers = newSafeList()
		}
		h.Unlock()
	}
	h.handlers.Lock()
	h.handlers.PushBack(handler)
	h.handlers.Unlock()
}

// AddHandlerFunc 添加处理器函数。
func (h *Handlers) AddHandlerFunc(f HandlerFunc) {
	h.AddHandler(f)
}

func (h *Handlers) defaultErrFunc(err error) (goon bool) {
	if err == nil || err == io.EOF {
		return true
	}
	return false
}

// Run 执行。
func (h *Handlers) Run() error {
	// 防止多次调用Run().
	// 初始化状态和停止状态都可以再次调用Run().
	h.Lock()
	if h.state == StatusRunning {
		h.Unlock()
		return errors.New("handlers already running")
	}
	h.state = StatusRunning

	if h.ErrCheck == nil {
		h.ErrCheck = h.defaultErrFunc
	}
	h.Unlock()

	for {
		src := h.popSrc()
		if src == nil {
			break
		}
		err := h.handleSrc(src)
		h.srcDone(src)
		if err != nil && !h.ErrCheck(err) {
			return err
		}
	}
	return nil
}

func (h *Handlers) handleSrc(src Source) error {
	if h.handlers == nil {
		return nil
	}
	h.handlers.RLock()
	defer h.handlers.RUnlock()

	for d, err := src.Next(); ; d, err = src.Next() {
		for e := h.handlers.Front(); e != nil; e = e.Next() {
			d, err = e.Value.(Handler).Handle(d)
		}
		if err != nil {
			return err
		}
	}
}
