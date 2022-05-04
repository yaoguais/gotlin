package gotlin

import (
	"context"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type ExecutorPool struct {
	ExecutorRepository ExecutorRepository

	is        *InstructionSet
	executors map[ExecutorID]bool
	ids       []ExecutorID
	mu        sync.RWMutex
}

func NewExecutorPool(er ExecutorRepository) *ExecutorPool {
	return &ExecutorPool{
		ExecutorRepository: er,
		is:                 NewInstructionSet(),
		executors:          make(map[ExecutorID]bool),
		ids:                []ExecutorID{},
		mu:                 sync.RWMutex{},
	}
}

func (m *ExecutorPool) AddServerExecutor() error {
	executor := NewExecutor()
	opcodes := []string{}
	for _, v := range DefaultInstructionHandlers {
		opcodes = append(opcodes, string(v.OpCode))
	}
	label := NewLabel(OpCodeLabelKey, strings.Join(opcodes, ","))
	executor = executor.AddLabel(label)
	executor, _ = executor.ChangeState(StateRunning)

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
	return
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
	m.mu.RLock()
	defer m.mu.RUnlock()

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

	return InstructionResult{}, errors.New("Remote Executor is not currently supported")
}
