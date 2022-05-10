package gotlin

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	opcodes := []string{}
	for _, v := range DefaultInstructionHandlers {
		opcodes = append(opcodes, string(v.OpCode))
	}
	label := NewLabel(OpCodeLabelKey, strings.Join(opcodes, ","))
	executor = executor.AddLabel(label)
	executor, _ = executor.ChangeState(StateRunning)
	return executor
}

func (m *ExecutorPool) AddServerExecutor() error {
	executor := m.getDefaultExecutor()
	err := m.Add(context.Background(), executor)
	return errors.Wrap(err, "Add server-side Executor")
}

func (m *ExecutorPool) Add(ctx context.Context, executor Executor) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exist := m.executors[executor.ID]
	if exist {
		return errors.Errorf("Executor already exists")
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
		return errors.Errorf("Executor not found, %v", id.String())
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

	return errors.Wrap(err, "Remove Executor")
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

	return Executor{}, errors.Errorf("Executor not found, opcode %s", op)
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
			errors.Errorf("Remote Executor not found, %v", executor.ID.String())
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
		return InstructionResult{}, errors.New("Remote Instruction execute timeout")
	}
	delete(m.rs, id)

	var result InstructionResult
	err := json.Unmarshal(data, &result)
	return result, err
}

type Commander struct {
	ep     *ExecutorPool
	host   Host
	stream ServerService_ExecuteCommandServer
}

func NewCommander(ep *ExecutorPool, host Host, stream ServerService_ExecuteCommandServer) *Commander {
	return &Commander{ep, host, stream}
}

func (c *Commander) ExecuteInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	ts := []*CommandToRemote_Instruction{}

	ins := append([]Instruction{}, op)
	ins = append(ins, args...)
	for _, in := range ins {
		operand, err := json.Marshal(in.Operand)
		if err != nil {
			return InstructionResult{}, err
		}
		result, err := json.Marshal(in.Result)
		if err != nil {
			return InstructionResult{}, err
		}
		t := &CommandToRemote_Instruction{
			Id:      in.ID.String(),
			Opcode:  string(in.OpCode),
			Operand: operand,
			Result:  result,
		}
		ts = append(ts, t)
	}

	id := NewID()
	timeout := int64(3000)

	r := &CommandToRemote{
		Id:      id.String(),
		Type:    CommandType_ExecuteInstruction,
		Timeout: timeout,
		ExecuteInstruction: &CommandToRemote_ExecuteInstruction{
			Op:   ts[0],
			Args: ts[1:],
		},
	}

	_ = c.ep.attachRemoteExecute(id)

	println("server send ==> ", r.String())
	err := c.stream.Send(r)
	if err != nil {
		return InstructionResult{}, errors.New("Send command to client")
	}
	time.Sleep(100 * time.Millisecond)

	return c.ep.getRemoteExecuteResult(id)
}
