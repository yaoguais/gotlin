package gotlin

import (
	"context"
	"sync"
	"time"
)

type Processor interface {
	Process(context.Context, Program) error
}

type pcProcessor struct {
	ExecutorPool          *ExecutorPool
	PCState               pcState
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
}

func newPCProcessor(pr ProgramRepository, ir InstructionRepository, ep *ExecutorPool) *pcProcessor {
	return &pcProcessor{
		ProgramRepository:     pr,
		InstructionRepository: ir,
		ExecutorPool:          ep,
	}
}

func (m *pcProcessor) Process(ctx context.Context, p Program) error {
	m.PCState = newPCState(ctx, &p, m.ProgramRepository)

	err := m.Loop(ctx)
	if err == ErrNoMoreInstruction {
		return nil
	}

	return err
}

func (m *pcProcessor) Loop(ctx context.Context) (err error) {
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
		return m.PCState.Finish(nil)
	}
	return m.PCState.Finish(err)
}

func (m *pcProcessor) Current(ctx context.Context) (Instruction, error) {

	for {
		id := m.PCState.Current()
		in, err := m.InstructionRepository.Find(ctx, id)
		if err != nil {
			return Instruction{}, err
		}

		ins := newPCInstructionState(ctx, &in, m.InstructionRepository)
		if ins.IsExecutable() {
			return in, ins.Run()
		}
		if err := ins.Error(); err != nil {
			return Instruction{}, err
		}

		if err := m.PCState.Next(); err != nil {
			return Instruction{}, err
		}
	}
}

func (m *pcProcessor) Execute(ctx context.Context, op Instruction) error {
	args, err := m.GetInstructionArgs(ctx, op)
	if err != nil {
		return err
	}

	result, execError := m.ExecutorPool.Execute(ctx, op, args...)

	err = m.SaveExecuteResult(ctx, op, result, execError)
	if err != nil {
		if execError != nil {
			return wrapError(execError, err.Error())
		}
		return wrapError(err, "Save results after executing instruction")
	}
	return wrapError(execError, "Executing instruction")
}

func (m *pcProcessor) GetInstructionArgs(ctx context.Context, op Instruction) ([]Instruction, error) {
	if IsReadWriteInstruction(op) {
		return []Instruction{}, nil
	}

	ids, err := m.PCState.Args()
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

func (m *pcProcessor) SaveExecuteResult(ctx context.Context, op Instruction, result InstructionResult, err error) error {
	in := op.Finish(result, err)
	return m.InstructionRepository.Save(ctx, &in)
}

func (m *pcProcessor) Next(ctx context.Context) error {
	return m.PCState.Next()
}

type pcState struct {
	ctx context.Context
	p   *Program
	r   ProgramRepository
}

func newPCState(ctx context.Context, p *Program, r ProgramRepository) pcState {
	return pcState{ctx: ctx, p: p, r: r}
}

func (m pcState) Validate(ctx context.Context) error {
	if !m.p.IsPCProcessor() {
		return wrapError(ErrProcessorType, "Current is %v", m.p.Processor.ControlUnit)
	}

	if !m.p.IsState(StateRunning) {
		return wrapError(ErrProgramState, "Is not running")
	}

	ins := m.p.Code.Instructions
	if len(ins) == 0 {
		return wrapError(ErrInstructions, "No nstruction found")
	}

	return nil
}

func (m pcState) Next() error {
	current := m.Current()

	ins := m.p.Code.Instructions
	n := len(ins)

	next := InstructionID{}
	found := false

	for i := 0; i < n-1; i++ {
		if ins[i].IsEqual(current) {
			next = ins[i+1]
			found = true
			break
		}
	}

	if !found {
		return ErrNoMoreInstruction
	}

	return m.saveCurrent(next)
}

func (m pcState) Current() InstructionID {
	pc, ok := m.p.Processor.CurrentPC()
	if ok {
		return pc
	}
	return m.p.Code.Instructions[0]
}

func (m pcState) IsLast(id InstructionID) bool {
	ins := m.p.Code.Instructions
	n := len(ins)

	return id.IsEqual(ins[n-1])
}

func (m pcState) saveCurrent(id InstructionID) error {
	p := m.p.NextPC(id)
	err := m.r.Save(m.ctx, &p)
	if err != nil {
		return err
	}
	*m.p = p
	return nil
}

func (m pcState) Args() ([]InstructionID, error) {
	current := m.Current()

	ins := m.p.Code.Instructions
	n := len(ins)

	i := 1
	for ; i < n; i++ {
		if current.IsEqual(ins[i]) {
			break
		}
	}
	if i == n {
		return nil, wrapError(ErrProgramState, "Args not found")
	}

	argc := 2
	j := i - argc
	if j < 0 {
		j = 0
	}

	args := []InstructionID{}
	for ; j < i; j++ {
		args = append(args, ins[j])
	}

	return args, nil
}

func (m pcState) Finish(exitErr error) error {
	p := m.p.ExitOnError(exitErr)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.r.Save(ctx, &p)
	if err != nil {
		return wrapError(err, "Exit error %v", exitErr)
	}
	*m.p = p
	return nil
}

type pcInstructionState struct {
	ctx context.Context
	in  *Instruction
	ir  InstructionRepository
}

func newPCInstructionState(ctx context.Context, in *Instruction, ir InstructionRepository) pcInstructionState {
	return pcInstructionState{ctx: ctx, in: in, ir: ir}
}

func (m pcInstructionState) IsExecutable() bool {
	return m.in.IsState(StateNew) || m.in.IsState(StateReady)
}

func (m pcInstructionState) Run() error {
	in, ok := m.in.ChangeState(StateRunning)
	if !ok {
		return wrapError(ErrInstructionState, "Change to running")
	}
	err := m.ir.Save(m.ctx, &in)
	if err != nil {
		return err
	}
	*m.in = in
	return nil
}

func (m pcInstructionState) Error() error {
	if m.in.IsState(StateExit) {
		return m.in.Error
	}
	return wrapError(ErrInstructionState, "Is not exit, %v", m.in.State)
}

type dagProcessor struct {
	ExecutorPool          *ExecutorPool
	DAGState              dagState
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
	InstructionDAG        InstructionDAG
	c                     chan InstructionID
	cores                 chan struct{}
	wg                    *sync.WaitGroup
	err                   error
	mu                    sync.RWMutex
}

func newDAGProcessor(pr ProgramRepository, ir InstructionRepository, ep *ExecutorPool) *dagProcessor {
	return &dagProcessor{
		ExecutorPool:          ep,
		ProgramRepository:     pr,
		InstructionRepository: ir,
		wg:                    &sync.WaitGroup{},
	}
}

func (m *dagProcessor) Process(ctx context.Context, p Program) error {
	defer m.wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dag, err := ParseInstructionDAG(p.Processor.Data)
	if err != nil {
		return err
	}

	m.InstructionDAG = dag
	m.DAGState = newDAGState(ctx, &p, m.ProgramRepository, dag, m.InstructionRepository)
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

func (m *dagProcessor) Walk(ctx context.Context) {
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

func (m *dagProcessor) Loop(ctx context.Context, p Program) (err error) {
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

func (m *dagProcessor) Current(ctx context.Context) (Instruction, error) {
	for {
		if err := m.Error(); err != nil {
			return Instruction{}, err
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

		ins := newDAGInstructionState(ctx, &in, m.InstructionRepository, m.InstructionDAG)
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

func (m *dagProcessor) Execute(ctx context.Context, op Instruction) error {
	if err := m.Error(); err != nil {
		return err
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
			m.setError(err)
			return
		}
	}()

	return nil
}

func (m *dagProcessor) execute(ctx context.Context, op Instruction) error {
	args, err := m.GetInstructionArgs(ctx, op)
	if err != nil {
		return err
	}

	result, execError := m.ExecutorPool.Execute(ctx, op, args...)

	err = m.SaveExecuteResult(ctx, op, result, execError)
	if err != nil {
		if execError != nil {
			return wrapError(execError, err.Error())
		}
		return wrapError(err, "Save results after executing instruction")
	}
	return wrapError(execError, "Executing instruction")
}

func (m *dagProcessor) GetInstructionArgs(ctx context.Context, op Instruction) ([]Instruction, error) {
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

func (m *dagProcessor) SaveExecuteResult(ctx context.Context, op Instruction, result InstructionResult, err error) error {
	in := op.Finish(result, err)
	return m.InstructionRepository.Save(ctx, &in)
}

func (m *dagProcessor) Next(ctx context.Context) error {
	if err := m.Error(); err != nil {
		return err
	}
	return m.DAGState.Next()
}

func (m *dagProcessor) RequestACore(ctx context.Context) error {
	select {
	case m.cores <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *dagProcessor) ReleaseACore(ctx context.Context) error {
	select {
	case <-m.cores:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *dagProcessor) Error() error {
	m.mu.RLock()
	err := m.err
	m.mu.RUnlock()
	return err
}

func (m *dagProcessor) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

type dagState struct {
	ctx  context.Context
	p    *Program
	r    ProgramRepository
	dag  InstructionDAG
	ir   InstructionRepository
	ans  []InstructionID
	exit *bool
	mu   *sync.RWMutex
}

func newDAGState(
	ctx context.Context, p *Program, pr ProgramRepository,
	dag InstructionDAG, ir InstructionRepository) dagState {
	exit := false
	ans := dag.Ancestors()
	return dagState{ctx, p, pr, dag, ir, ans, &exit, &sync.RWMutex{}}
}

func (m dagState) Finish(exitErr error) error {
	m.finish()

	p := m.p.ExitOnError(exitErr)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := m.r.Save(ctx, &p)
	if err != nil {
		return wrapError(err, "Exit error %v", exitErr)
	}
	*m.p = p
	return nil
}

func (m dagState) finish() {
	m.mu.Lock()
	*m.exit = true
	m.mu.Unlock()
}

func (m dagState) IsFinish() bool {
	m.mu.RLock()
	exit := *m.exit
	m.mu.RUnlock()
	return exit
}

func (m dagState) Next() (err error) {
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

type dagInstructionState struct {
	ctx context.Context
	in  *Instruction
	ir  InstructionRepository
	dag InstructionDAG
}

func newDAGInstructionState(
	ctx context.Context, in *Instruction,
	ir InstructionRepository, dag InstructionDAG) dagInstructionState {

	return dagInstructionState{ctx, in, ir, dag}
}

func (m dagInstructionState) IsExecutable() (ok bool, err error) {
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

func (m dagInstructionState) Run() error {
	in, ok := m.in.ChangeState(StateRunning)
	if !ok {
		return wrapError(ErrInstructionState, "Change to running")
	}
	err := m.ir.Save(m.ctx, &in)
	if err != nil {
		return err
	}
	*m.in = in
	return nil
}

func (m dagInstructionState) Error() error {
	if m.in.IsState(StateBlocked) {
		return wrapError(ErrInstructionState, "Is blocked, %v", m.in.State)
	}
	return m.in.Error
}
