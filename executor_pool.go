package gotlin

import (
	"context"
	"sync"

	. "github.com/yaoguais/gotlin/proto"
)

type ExecutorPool struct {
	ExecutorRepository ExecutorRepository

	is        *InstructionSet
	executors map[ExecutorID]bool
	ids       []ExecutorID
	hs        map[Host]ExecutorID
	cs        map[Host]*executor
	mu        sync.RWMutex
}

func NewExecutorPool(er ExecutorRepository, is *InstructionSet) *ExecutorPool {
	return &ExecutorPool{
		ExecutorRepository: er,
		is:                 is,
		executors:          make(map[ExecutorID]bool),
		ids:                []ExecutorID{},
		hs:                 make(map[Host]ExecutorID),
		cs:                 make(map[Host]*executor),
		mu:                 sync.RWMutex{},
	}
}

func (m *ExecutorPool) AddServerExecutor() error {
	executor := NewExecutor()
	executor = executor.AddLabel(m.is.OpCodeLabel())
	executor, _ = executor.ChangeState(StateRunning)
	err := m.Add(context.Background(), executor)
	return wrapError(err, "Add server-side Executor")
}

func (m *ExecutorPool) Add(ctx context.Context, executor Executor) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exist := m.executors[executor.ID]
	if exist {
		return newErrorf("Executor already exists")
	}

	err = m.ExecutorRepository.Save(ctx, &executor)
	if err != nil {
		return
	}

	m.executors[executor.ID] = true
	m.ids = append(m.ids, executor.ID)
	if !executor.IsEmptyHost() {
		m.hs[executor.Host] = executor.ID
	}
	return
}

func (m *ExecutorPool) Remove(ctx context.Context, id ExecutorID, removeErr error) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := false
	ids := []ExecutorID{}
	for _, v := range m.ids {
		if v.IsEqual(id) {
			found = true
		} else {
			ids = append(ids, v)
		}
	}
	m.ids = ids

	if !found {
		return newErrorf("Executor not found, %v", id.String())
	}

	executor, err := m.ExecutorRepository.Find(ctx, id)
	if err != nil {
		return
	}
	if !executor.IsEmptyHost() {
		delete(m.hs, executor.Host)
	}
	executor = executor.ExitOnError(removeErr)
	err = m.ExecutorRepository.Save(ctx, &executor)

	return wrapError(err, "Remove Executor")
}

func (m *ExecutorPool) FindByHost(ctx context.Context, host Host) (ExecutorID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, ok := m.hs[host]
	if !ok {
		return ExecutorID{}, ErrNotFound
	}
	return id, nil
}

func (m *ExecutorPool) GetExecuteHandler(ctx context.Context, op OpCode) (eh ExecutorHandler, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, found := Executor{}, false

	for i := len(m.ids) - 1; i >= 0; i-- {
		id := m.ids[i]
		e, err = m.ExecutorRepository.Find(ctx, id)
		if err != nil {
			return nil, err
		}
		ok := e.IsState(StateRunning) && e.Labels.ExistOpCode(op)
		if ok {
			found = true
			break
		}
	}

	if !found {
		return nil, newErrorf("Executor not found, opcode %s", op)
	}

	if e.IsEmptyHost() {
		return m.is.GetExecutorHandler(op)
	}

	ec, ok := m.cs[e.Host]
	if !ok {
		return nil, newErrorf("Remote Executor %s not found", e.ID)
	}

	return ec.Execute, nil
}

func (m *ExecutorPool) Execute(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	handler, err := m.GetExecuteHandler(ctx, op.OpCode)
	if err != nil {
		return InstructionResult{}, err
	}
	return handler(ctx, op, args...)
}

func (m *ExecutorPool) Attach(c *executor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cs[c.host] = c
	return nil
}

func (m *ExecutorPool) Detach(c *executor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.cs[c.host]
	if !ok {
		return wrapError(ErrNotFound, "Executor %s", c.host)
	}
	delete(m.cs, c.host)

	return nil
}

type executor struct {
	ep     *ExecutorPool
	host   Host
	stream ServerService_ExecuteServer
	o      sync.Once
	sub    map[string]*subInstructionResult
	mu     sync.Mutex
	l      serverLogger
	formatError
}

func newExecutor(ep *ExecutorPool, host Host, stream ServerService_ExecuteServer, l serverLogger, fe formatError) *executor {
	o := sync.Once{}
	mu := sync.Mutex{}
	sub := make(map[string]*subInstructionResult)
	return &executor{ep, host, stream, o, sub, mu, l, fe}
}

func (e *executor) Execute(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	e.preExecuting()

	id := NewExecuteID()

	l := e.l.WithExecuteID(id.String()).WithExecute(op, args)
	logger := l.Logger()
	logger.Info("Find compute nodes and execute instructions")

	ins := []Instructioner{}
	for _, v := range args {
		ins = append(ins, v)
	}

	timeout := int64(3000) // TODO

	pc := pbConverter{}

	opPb, err := pc.InstructionerToPb(op)
	if err != nil {
		return InstructionResult{}, err
	}

	argsPb, err := pc.InstructionersToPb(ins)
	if err != nil {
		return InstructionResult{}, err
	}

	r := &ExecuteStream{
		Id:      id.String(),
		Type:    ExecuteStream_Execute,
		Timeout: timeout,
		Op:      opPb,
		Args:    argsPb,
	}

	logger.Debugf("Send an instruction to the compute node, %s", r)

	sub := newSubInstructionResult()
	e.mu.Lock()
	e.sub[r.Id] = sub
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.sub, r.Id)
		e.mu.Unlock()
		sub.Close()
	}()

	err = e.stream.Send(r)
	if err != nil {
		return InstructionResult{}, newError("Send command to client")
	}

	return <-sub.ch, <-sub.errCh
}

func (e *executor) preExecuting() {
	e.o.Do(func() {
		go e.waitLoop()
	})
}

func (e *executor) waitLoop() {
	for {
		id, ir, err := e.waitResult()
		e.mu.Lock()
		sub, ok := e.sub[id]
		e.mu.Unlock()
		if ok {
			sub.Send(ir, err)
		}
	}
}

func (e *executor) waitResult() (id string, ir InstructionResult, err error) {
	r, err := e.stream.Recv()
	if err != nil {
		return "", InstructionResult{},
			e.error(ErrReceive, err, "Read instruction execution results from computing nodes")
	}

	if r.Type != ExecuteStream_Result {
		return "", InstructionResult{},
			e.error(ErrResponse, ErrUndoubted, "Server receive invalid type "+r.Type.String())
	}

	l := e.l.WithExecuteID(r.Id)
	logger := l.Logger()

	logger.Debugf("Received the result of an instruction, %s", r)

	pc := pbConverter{}
	iner, err := pc.InstructionToModel(r.Result)
	if err != nil {
		return r.Id, InstructionResult{}, err
	}
	in := iner.Instruction()

	return r.Id, in.Result, nil
}

type subInstructionResult struct {
	ch     chan InstructionResult
	errCh  chan error
	mu     sync.Mutex
	closed bool
}

func newSubInstructionResult() *subInstructionResult {
	return &subInstructionResult{
		ch:     make(chan InstructionResult, 1),
		errCh:  make(chan error, 1),
		mu:     sync.Mutex{},
		closed: false,
	}
}

func (s *subInstructionResult) Send(v InstructionResult, err error) {
	s.mu.Lock()
	if !s.closed {
		s.ch <- v
		s.errCh <- err
	}
	s.mu.Unlock()
}

func (s *subInstructionResult) Close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.ch)
		close(s.errCh)
	}
	s.mu.Unlock()
}
