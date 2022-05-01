package gotlin

import (
	"context"

	"github.com/pkg/errors"
)

type Processor interface {
	Proccess(context.Context, Program) error
}

type PCProcessor struct {
	InstructionSet        *InstructionSet
	PCState               PCState
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
}

func NewPCProcessor(pr ProgramRepository, ir InstructionRepository, is *InstructionSet) *PCProcessor {
	return &PCProcessor{
		ProgramRepository:     pr,
		InstructionRepository: ir,
		InstructionSet:        is,
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
	executor, err := m.InstructionSet.GetExecutor(op.OpCode)
	if err != nil {
		return err
	}

	args, err := m.GetInstructionArgs(ctx, op)
	if err != nil {
		return err
	}

	result, execError := executor(ctx, op, args...)

	err = m.SaveExecuteResult(ctx, op, result, execError)
	if err != nil {
		return errors.Wrap(execError, err.Error())
	}
	return execError
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
