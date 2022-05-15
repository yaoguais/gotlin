package gotlin

import (
	"context"
	"time"
)

type Processor interface {
	Process(context.Context, Program) error
}

type PCProcessor struct {
	ExecutorPool          *ExecutorPool
	PCState               PCState
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
}

func NewPCProcessor(pr ProgramRepository, ir InstructionRepository, ep *ExecutorPool) *PCProcessor {
	return &PCProcessor{
		ProgramRepository:     pr,
		InstructionRepository: ir,
		ExecutorPool:          ep,
	}
}

func (m *PCProcessor) Process(ctx context.Context, p Program) error {
	m.PCState = NewPCState(ctx, &p, m.ProgramRepository)

	err := m.Loop(ctx)
	if err == ErrNoMoreInstruction {
		return nil
	}

	return err
}

func (m *PCProcessor) Loop(ctx context.Context) (err error) {
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

func (m *PCProcessor) Current(ctx context.Context) (Instruction, error) {

	for {
		id := m.PCState.Current()
		in, err := m.InstructionRepository.Find(ctx, id)
		if err != nil {
			return Instruction{}, err
		}

		ins := NewPCInstructionState(ctx, &in, m.InstructionRepository)
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

func (m *PCProcessor) Execute(ctx context.Context, op Instruction) error {
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

func (m *PCProcessor) GetInstructionArgs(ctx context.Context, op Instruction) ([]Instruction, error) {
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

func (m *PCProcessor) SaveExecuteResult(ctx context.Context, op Instruction, result InstructionResult, err error) error {
	in := op.Finish(result, err)
	return m.InstructionRepository.Save(ctx, &in)
}

func (m *PCProcessor) Next(ctx context.Context) error {
	return m.PCState.Next()
}

type PCState struct {
	ctx context.Context
	p   *Program
	r   ProgramRepository
}

func NewPCState(ctx context.Context, p *Program, r ProgramRepository) PCState {
	return PCState{ctx: ctx, p: p, r: r}
}

func (m PCState) Validate(ctx context.Context) error {
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

func (m PCState) Next() error {
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

func (m PCState) Current() InstructionID {
	pc, ok := m.p.Processor.CurrentPC()
	if ok {
		return pc
	}
	return m.p.Code.Instructions[0]
}

func (m PCState) IsLast(id InstructionID) bool {
	ins := m.p.Code.Instructions
	n := len(ins)

	return id.IsEqual(ins[n-1])
}

func (m PCState) saveCurrent(id InstructionID) error {
	p := m.p.NextPC(id)
	err := m.r.Save(m.ctx, &p)
	if err != nil {
		return err
	}
	*m.p = p
	return nil
}

func (m PCState) Args() ([]InstructionID, error) {
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

func (m PCState) Finish(exitErr error) error {
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

type PCInstructionState struct {
	ctx context.Context
	in  *Instruction
	ir  InstructionRepository
}

func NewPCInstructionState(ctx context.Context, in *Instruction, ir InstructionRepository) PCInstructionState {
	return PCInstructionState{ctx: ctx, in: in, ir: ir}
}

func (m PCInstructionState) IsExecutable() bool {
	return m.in.IsState(StateNew) || m.in.IsState(StateReady)
}

func (m PCInstructionState) Run() error {
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

func (m PCInstructionState) Error() error {
	if m.in.IsState(StateExit) {
		return m.in.Error
	}
	return wrapError(ErrInstructionState, "Is not exit, %v", m.in.State)
}
