package gotlin

import "github.com/pkg/errors"

var (
	ErrProcessorType     = errors.New("Proccess is invalid")
	ErrProgramState      = errors.New("Program state is invalid")
	ErrInstructionState  = errors.New("Instruction state is invalid")
	ErrInstructions      = errors.New("Instructions is invalid")
	ErrNoMoreInstruction = errors.New("No more instruction found")
)
