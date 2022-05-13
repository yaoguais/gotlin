package gotlin

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type DAGProcessor struct {
	ExecutorPool          *ExecutorPool
	DAGState              DAGState
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
	InstructionDAG        InstructionDAG
	c                     chan InstructionID
	cores                 chan struct{}
	wg                    *sync.WaitGroup
	err                   error
}

func NewDAGProcessor(pr ProgramRepository, ir InstructionRepository, ep *ExecutorPool) *DAGProcessor {
	return &DAGProcessor{
		ExecutorPool:          ep,
		ProgramRepository:     pr,
		InstructionRepository: ir,
		wg:                    &sync.WaitGroup{},
	}
}

func (m *DAGProcessor) Process(ctx context.Context, p Program) error {
	defer m.wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dag, err := ParseInstructionDAG(p.Processor.Data)
	if err != nil {
		return err
	}

	m.InstructionDAG = dag
	m.DAGState = NewDAGState(ctx, &p, m.ProgramRepository, dag, m.InstructionRepository)
	m.c = make(chan InstructionID)
	m.cores = make(chan struct{}, p.Processor.Core)

	m.wg.Add(1)
	go m.Walk(ctx)

	err = m.Loop(ctx, p)
	if err == ErrNoMoreInstruction {
		return nil
	}

	return err
}

func (m *DAGProcessor) Walk(ctx context.Context) {
	defer close(m.c)
	defer m.wg.Done()

	for !m.DAGState.IsFinish() {
		for in := range m.InstructionDAG.Iterator(ctx) {
			select {
			case m.c <- in:
				if m.DAGState.IsFinish() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *DAGProcessor) Loop(ctx context.Context, p Program) (err error) {
	var in Instruction

	for {
		in, err = m.Current(ctx)
		if err != nil {
			break
		}

		err = m.Execute(ctx, in)
		if err != nil {
			break
		}

		err = m.Next(ctx)
		if err != nil {
			break
		}
	}

	if err == ErrNoMoreInstruction {
		return m.DAGState.Finish(nil)
	}
	return m.DAGState.Finish(err)
}

func (m *DAGProcessor) Current(ctx context.Context) (Instruction, error) {
	for {
		if m.err != nil {
			return Instruction{}, m.err
		}

		id, ok := InstructionID{}, true
		select {
		case id, ok = <-m.c:
		case <-ctx.Done():
			return Instruction{}, ctx.Err()
		}

		if !ok {
			return Instruction{}, ErrNoMoreInstruction
		}

		in, err := m.InstructionRepository.Find(ctx, id)
		if err != nil {
			return Instruction{}, err
		}

		ins := NewDAGInstructionState(ctx, &in, m.InstructionRepository, m.InstructionDAG)
		ok, err = ins.IsExecutable()
		if err != nil {
			return Instruction{}, err
		}
		if ok {
			return in, ins.Run()
		}

		if err := ins.Error(); err != nil {
			return Instruction{}, err
		}

		if err := m.DAGState.Next(); err != nil {
			return Instruction{}, err
		}
	}
}

func (m *DAGProcessor) Execute(ctx context.Context, op Instruction) error {
	if m.err != nil {
		return m.err
	}

	err := m.RequestACore(ctx)
	if err != nil {
		return err
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.ReleaseACore(ctx)

		err := m.execute(ctx, op)
		if err != nil {
			m.err = err
			return
		}
	}()

	return nil
}

func (m *DAGProcessor) execute(ctx context.Context, op Instruction) error {
	args, err := m.GetInstructionArgs(ctx, op)
	if err != nil {
		return err
	}

	result, execError := m.ExecutorPool.Execute(ctx, op, args...)

	err = m.SaveExecuteResult(ctx, op, result, execError)
	if err != nil {
		if execError != nil {
			return errors.Wrap(execError, err.Error())
		}
		return errors.Wrap(err, "Save results after executing instruction")
	}
	return errors.Wrap(execError, "Executing instruction")
}

func (m *DAGProcessor) GetInstructionArgs(ctx context.Context, op Instruction) ([]Instruction, error) {
	if IsReadWriteInstruction(op) {
		return []Instruction{}, nil
	}

	ids, err := m.InstructionDAG.Children(op.ID)
	if err != nil {
		return nil, err
	}

	args := []Instruction{}
	for _, id := range ids {
		in, err := m.InstructionRepository.Find(ctx, id)
		if err != nil {
			return nil, err
		}
		args = append(args, in)
	}

	return args, nil
}

func (m *DAGProcessor) SaveExecuteResult(ctx context.Context, op Instruction, result InstructionResult, err error) error {
	in := op.Finish(result, err)
	return m.InstructionRepository.Save(ctx, &in)
}

func (m *DAGProcessor) Next(ctx context.Context) error {
	if m.err != nil {
		return m.err
	}
	return m.DAGState.Next()
}

func (m *DAGProcessor) RequestACore(ctx context.Context) error {
	select {
	case m.cores <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *DAGProcessor) ReleaseACore(ctx context.Context) error {
	select {
	case <-m.cores:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type DAGState struct {
	ctx  context.Context
	p    *Program
	r    ProgramRepository
	dag  InstructionDAG
	ir   InstructionRepository
	ans  []InstructionID
	exit *bool
	mu   *sync.RWMutex
}

func NewDAGState(
	ctx context.Context, p *Program, pr ProgramRepository,
	dag InstructionDAG, ir InstructionRepository) DAGState {
	exit := false
	ans := dag.Ancestors()
	return DAGState{ctx, p, pr, dag, ir, ans, &exit, &sync.RWMutex{}}
}

func (m DAGState) Finish(exitErr error) error {
	m.finish()

	p := m.p.ExitOnError(exitErr)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.r.Save(ctx, &p)
	if err != nil {
		return errors.Wrapf(err, "Exit error %v", exitErr)
	}
	*m.p = p
	return nil
}

func (m DAGState) finish() {
	m.mu.Lock()
	*m.exit = true
	m.mu.Unlock()
}

func (m DAGState) IsFinish() bool {
	m.mu.RLock()
	exit := *m.exit
	m.mu.RUnlock()
	return exit
}

func (m DAGState) Next() (err error) {
	ancestorsDone := true

	var in Instruction
	for _, id := range m.ans {
		in, err = m.ir.Find(m.ctx, id)
		if err != nil {
			return
		}
		if err = in.Error; err != nil {
			return
		}
		if !in.IsState(StateExit) {
			ancestorsDone = false
		}
	}

	if ancestorsDone {
		m.finish()
		return ErrNoMoreInstruction
	}

	return nil
}

type DAGInstructionState struct {
	ctx context.Context
	in  *Instruction
	ir  InstructionRepository
	dag InstructionDAG
}

func NewDAGInstructionState(
	ctx context.Context, in *Instruction,
	ir InstructionRepository, dag InstructionDAG) DAGInstructionState {

	return DAGInstructionState{ctx, in, ir, dag}
}

func (m DAGInstructionState) IsExecutable() (ok bool, err error) {
	ok = m.in.IsState(StateNew) || m.in.IsState(StateReady)
	if !ok {
		return
	}
	ok = false

	ids, err := m.dag.Children(m.in.ID)
	if err != nil {
		return
	}

	chilrenDone := true

	var in Instruction
	for _, id := range ids {
		in, err = m.ir.Find(m.ctx, id)
		if err != nil {
			return
		}
		if err = in.Error; err != nil {
			return
		}
		if !in.IsState(StateExit) {
			chilrenDone = false
		}
	}

	return chilrenDone, nil
}

func (m DAGInstructionState) Run() error {
	in, ok := m.in.ChangeState(StateRunning)
	if !ok {
		return errors.Wrap(ErrInstructionState, "Change to running")
	}
	err := m.ir.Save(m.ctx, &in)
	if err != nil {
		return err
	}
	*m.in = in
	return nil
}

func (m DAGInstructionState) Error() error {
	if m.in.IsState(StateBlocked) {
		return errors.Wrapf(ErrInstructionState, "Is blocked, %v", m.in.State)
	}
	return m.in.Error
}
