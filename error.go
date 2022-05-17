package gotlin

import (
	"github.com/pkg/errors"
)

// Common Errors
var (
	ErrUndoubted = newError("Undoubted")
)

// Repository Errors
var (
	ErrFind       = newError("Find")
	ErrNotFound   = newError("Not found")
	ErrConflict   = newError("Conflict")
	ErrPersistent = newError("Persistent")
)

// Client Errors
var (
	ErrConnect          = newError("Connect")
	ErrRequest          = newError("Request")
	ErrExitUnexpectedly = newError("Exit unexpectedly")
	ErrConverter        = newError("Converter")
	ErrNilPointer       = newError("NilPointer")
)

// Service Errors
var (
	ErrResponse = newError("Response")
	ErrReceive  = newError("Receive")
	ErrSend     = newError("Send")
)

// Executor Errors
var (
	ErrExecutorNotFound = newError("Executor not found")
)

// InstructionSet Errors
var (
	ErrRegisterResult   = newError("Only register result is supported for arithmetic instructions")
	ErrArithmeticOpCode = newError("Only ADD/SUB/MUL/DIV are supported for arithmetic opcode")
)

// Model Errors
var (
	ErrProcessorType         = newError("Processor type is invalid")
	ErrProgramState          = newError("Program state is invalid")
	ErrProgramCode           = newError("Program code is invalid")
	ErrProgramResult         = newError("Program result is invalid")
	ErrInstructionState      = newError("Instruction state is invalid")
	ErrInstructions          = newError("Instructions is invalid")
	ErrSchedulerDuplicated   = newError("Scheduler duplicated")
	ErrProgramDuplicated     = newError("Program duplicated")
	ErrInstructionDuplicated = newError("Instruction duplicated")
)

// Processor Errors
var (
	ErrNoMoreInstruction = newError("No more instruction found")
)

func newError(message string) error {
	return errors.New(message)
}

func newErrorf(format string, args ...interface{}) error {
	return errors.Errorf(format, args...)
}

func wrapError(err error, format string, args ...interface{}) error {
	return errors.Wrapf(err, format, args...)
}

type formatError struct {
	format string
}

func (e formatError) error(cause error, err error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	args = append(args, err)
	return wrapError(cause, e.format+" %v", args...)
}
