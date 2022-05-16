package gotlin

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type ExecutorPool struct {
	ExecutorRepository ExecutorRepository

	is        *InstructionSet
	executors map[ExecutorID]bool
	ids       []ExecutorID
	hs        map[Host]ExecutorID
	cs        map[Host]*Commander
	rs        map[ID][]byte
	mu        sync.RWMutex
}

func NewExecutorPool(er ExecutorRepository) *ExecutorPool {
	return &ExecutorPool{
		ExecutorRepository: er,
		is:                 NewInstructionSet(),
		executors:          make(map[ExecutorID]bool),
		ids:                []ExecutorID{},
		hs:                 make(map[Host]ExecutorID),
		cs:                 make(map[Host]*Commander),
		rs:                 make(map[ID][]byte),
		mu:                 sync.RWMutex{},
	}
}

func (m *ExecutorPool) getDefaultExecutor() Executor {
	executor := NewExecutor()
	executor = executor.AddLabel(NewDefaultOpCodeLabel())
	executor, _ = executor.ChangeState(StateRunning)
	return executor
}

func (m *ExecutorPool) AddServerExecutor() error {
	executor := m.getDefaultExecutor()
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
		if v == id {
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

func (m *ExecutorPool) find(ctx context.Context, op OpCode) (Executor, error) {
	for _, id := range m.ids {
		e, err := m.ExecutorRepository.Find(ctx, id)
		if err != nil {
			return Executor{}, err
		}
		ok := e.IsState(StateRunning) && e.Labels.ExistOpCode(op)
		if ok {
			return e, nil
		}
	}

	return Executor{}, newErrorf("Executor not found, opcode %s", op)
}

func (m *ExecutorPool) Execute(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	// m.mu.RLock()
	// defer m.mu.RUnlock()

	executor, err := m.find(ctx, op.OpCode)
	if err != nil {
		return InstructionResult{}, err
	}

	if executor.IsEmptyHost() {
		handler, err := m.is.GetExecutorHandler(op.OpCode)
		if err != nil {
			return InstructionResult{}, err
		}
		return handler(ctx, op, args...)
	}

	commander, ok := m.cs[executor.Host]
	if !ok {
		return InstructionResult{},
			newErrorf("Remote Executor not found, %v", executor.ID.String())
	}

	return commander.ExecuteInstruction(ctx, op, args...)
}

func (m *ExecutorPool) attachCommander(c *Commander) error {
	m.cs[c.host] = c
	return nil
}

func (m *ExecutorPool) attachRemoteExecute(id ID) error {
	m.rs[id] = nil
	return nil
}

func (m *ExecutorPool) setRemoteExecuteResult(id ID, result []byte) error {
	if result == nil {
		result = []byte{}
	}

	m.rs[id] = result

	return nil
}

func (m *ExecutorPool) getRemoteExecuteResult(id ID) (InstructionResult, error) {
	data, ok := m.rs[id]
	if !ok {
		return InstructionResult{}, newError("Remote Instruction execute timeout")
	}
	delete(m.rs, id)

	var result InstructionResult
	err := json.Unmarshal(data, &result)
	return result, err
}

type Commander struct {
	ep     *ExecutorPool
	host   Host
	stream ServerService_ExecuteServer
}

func NewCommander(ep *ExecutorPool, host Host, stream ServerService_ExecuteServer) *Commander {
	return &Commander{ep, host, stream}
}

func (c *Commander) ExecuteInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	ins := []Instructioner{}
	for _, v := range args {
		ins = append(ins, v)
	}

	id := NewID()
	timeout := int64(3000)

	pc := pbConverter{}

	r := &ExecuteStream{
		Id:      id.String(),
		Type:    ExecuteStream_Execute,
		Timeout: timeout,
		Op:      pc.InstructionerToPb(op),
		Args:    pc.InstructionersToPb(ins),
	}

	_ = c.ep.attachRemoteExecute(id)

	println("server send ==> ", r.String())
	err := c.stream.Send(r)
	if err != nil {
		return InstructionResult{}, newError("Send command to client")
	}
	time.Sleep(100 * time.Millisecond)

	return c.ep.getRemoteExecuteResult(id)
}
