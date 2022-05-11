package gotlin

import "github.com/pkg/errors"

var (
	ErrNotFound              = errors.New("Record not found")
	ErrProcessorType         = errors.New("Proccess is invalid")
	ErrProgramState          = errors.New("Program state is invalid")
	ErrInstructionState      = errors.New("Instruction state is invalid")
	ErrInstructions          = errors.New("Instructions is invalid")
	ErrNoMoreInstruction     = errors.New("No more instruction found")
	ErrSchedulerDuplicated   = errors.New("Scheduler duplicated")
	ErrProgramDuplicated     = errors.New("Program duplicated")
	ErrInstructionDuplicated = errors.New("Instruction duplicated")
)
