package gotlin

import (
	"context"

	"github.com/pkg/errors"
)

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
		return errors.Wrap(ErrInstructionState, "Change to running")
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
	return errors.Wrapf(ErrInstructionState, "Is not exit, %v", m.in.State)
}
