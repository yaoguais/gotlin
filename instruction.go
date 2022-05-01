package gotlin

import (
	"context"

	"github.com/pkg/errors"
)

type Instruction struct {
	ID         InstructionID
	OpCode     OpCode
	Operand    Operand
	Result     InstructionResult
	State      State
	Error      error
	CreateTime Timestamp
	UpdateTime Timestamp
	FinishTime Timestamp
}

func NewInstruction() Instruction {
	return Instruction{
		ID:         NewInstructionID(),
		OpCode:     OpCodeMove,
		Operand:    NewImmediateValue(0),
		Result:     NewEmptyInstructionResult(),
		State:      StateNew,
		Error:      nil,
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}
}

func (m Instruction) OperandValue(ctx context.Context) (interface{}, error) {
	return m.Operand.OperandValue(ctx)
}

func (m Instruction) InstructionResult(ctx context.Context) (interface{}, error) {
	return m.Result.InstructionResult(ctx)
}

func (m Instruction) IsState(state State) bool {
	return m.State == state
}

func (m Instruction) ChangeState(state State) (Instruction, bool) {
	m.State = state
	return m, true
}

func (m Instruction) ChangeImmediateValue(value interface{}) Instruction {
	m.OpCode = OpCodeMove
	m.Operand = NewImmediateValue(value)
	return m
}

func (m Instruction) ChangeDatabaseQuery(v DatabaseQuery) Instruction {
	m.OpCode = OpCodeIn
	m.Operand = NewDatabaseQueryOperand(v)
	return m
}

func (m Instruction) ChangeToArithmetic(op OpCode) Instruction {
	m.OpCode = op
	m.Operand = NewEmptyOperand()
	return m
}

func (m Instruction) Finish(result InstructionResult, err error) Instruction {
	m.State = StateExit
	if err != nil {
		if m.Error == nil {
			m.Error = err
		} else {
			m.Error = errors.Wrap(err, m.Error.Error())
		}
	} else {
		m.Result = result
	}
	m.FinishTime = NewTimestamp()

	return m
}
