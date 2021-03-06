package gotlin

import (
	"context"
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

func (m Instruction) ChangeToWait(value interface{}) Instruction {
	m.OpCode = OpCodeWait
	m.Operand = NewImmediateValue(value)
	return m
}

func (m Instruction) ChangeDatabaseInput(v DatabaseInput) Instruction {
	m.OpCode = OpCodeIn
	m.Operand = NewDatabaseInputOperand(v)
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
			m.Error = wrapErrorf(err, m.Error.Error())
		}
	} else {
		m.Result = result
	}
	m.FinishTime = NewTimestamp()

	return m
}

func (m Instruction) Reready() Instruction {
	m.State = StateReady
	m.Error = nil
	m.FinishTime = TimestampZero
	return m
}

func (m Instruction) Instruction() Instruction {
	return m
}

type Instructioner interface {
	Instruction() Instruction
}

type InstructionRefer interface {
	Instructioner
	IsRef()
}

type InstructionRef struct {
	in Instruction
}

func NewInstructionRef(in Instruction) InstructionRef {
	in.ID = in.ID.changeNonce()
	return InstructionRef{in}
}

func (m InstructionRef) Instruction() Instruction {
	return m.in
}

func (m InstructionRef) IsRef() {
}
