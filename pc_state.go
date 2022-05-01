package gotlin

import (
	"context"

	"github.com/pkg/errors"
)

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
		return errors.Wrapf(ErrProcessorType, "Current is %v", m.p.Processor.ControlUnit)
	}

	if !m.p.IsState(StateRunning) {
		return errors.Wrap(ErrProgramState, "Is not running")
	}

	ins := m.p.Code.Instructions
	if len(ins) == 0 {
		return errors.Wrap(ErrInstructions, "No nstruction found")
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
		return nil, errors.Wrap(ErrProgramState, "Args not found")
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
	err := m.r.Save(m.ctx, &p)
	if err != nil {
		return errors.Wrapf(err, "Exit error %v", exitErr)
	}
	*m.p = p
	return nil
}
