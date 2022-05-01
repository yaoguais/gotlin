package gotlin

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var ErrNoExecutorFound = errors.New("No executor found")

type Executor func(context.Context, Instruction, ...Instruction) (InstructionResult, error)

type InstructionHandler struct {
	OpCode   OpCode
	Executor Executor
}

type InstructionSet struct {
	handlers map[OpCode]InstructionHandler
	mu       sync.RWMutex
}

func NewInstructionSet() *InstructionSet {
	m := &InstructionSet{
		handlers: make(map[OpCode]InstructionHandler),
		mu:       sync.RWMutex{},
	}
	m.registerDefaults()
	return m
}

func (m *InstructionSet) Register(handler InstructionHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exist := m.handlers[handler.OpCode]
	if exist {
		return errors.Errorf("Instruction handler %s is already exist", handler.OpCode)
	}
	m.handlers[handler.OpCode] = handler
	return nil
}

func (m *InstructionSet) Unregister(handler InstructionHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exist := m.handlers[handler.OpCode]
	if exist {
		delete(m.handlers, handler.OpCode)
	}
	return nil
}

func (m *InstructionSet) GetExecutor(op OpCode) (Executor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.handlers[op]
	if ok {
		return v.Executor, nil
	}
	return nil, ErrNoExecutorFound
}

func (m *InstructionSet) ClearDefaults() {
	for _, handler := range DefaultInstructionHandlers {
		_ = m.Unregister(handler)
	}
}

func (m *InstructionSet) registerDefaults() {
	for _, handler := range DefaultInstructionHandlers {
		_ = m.Register(handler)
	}
}

var (
	MoveInstructionHandler = InstructionHandler{
		OpCode:   OpCodeMove,
		Executor: ExecuteMoveInstruction,
	}
	InputInstructionHandler = InstructionHandler{
		OpCode:   OpCodeIn,
		Executor: ExecuteInputInstruction,
	}
	AddInstructionHandler = InstructionHandler{
		OpCode:   OpCodeAdd,
		Executor: ExecuteArithmeticInstruction,
	}
	SubInstructionHandler = InstructionHandler{
		OpCode:   OpCodeSub,
		Executor: ExecuteArithmeticInstruction,
	}
	MulInstructionHandler = InstructionHandler{
		OpCode:   OpCodeMul,
		Executor: ExecuteArithmeticInstruction,
	}
	DivInstructionHandler = InstructionHandler{
		OpCode:   OpCodeDiv,
		Executor: ExecuteArithmeticInstruction,
	}
)

var DefaultInstructionHandlers = []InstructionHandler{
	MoveInstructionHandler,
	InputInstructionHandler,
	AddInstructionHandler,
	SubInstructionHandler,
	MulInstructionHandler,
	DivInstructionHandler,
}

var ErrRegisterResult = errors.New("Only register result is supported for arithmetic instructions")
var ErrArithmeticOpCode = errors.New("Only ADD/SUB/MUL/DIV are supported for arithmetic opcode")

func ExecuteArithmeticInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	var result float64

	n := len(args)
	if n > 0 {
		in := args[0]
		tmp, err := in.InstructionResult(ctx)
		if err != nil {
			return InstructionResult{},
				errors.Wrapf(err, "Get arithmetic instruction operands, %v", in.ID.String())
		}
		result = cast.ToFloat64(tmp)
	}

	for i := 1; i < n; i++ {
		in := args[i]
		tmp, err := in.InstructionResult(ctx)
		if err != nil {
			return InstructionResult{},
				errors.Wrapf(err, "Get arithmetic instruction operands, %v", in.ID.String())
		}

		x := cast.ToFloat64(tmp)

		switch op.OpCode {
		case OpCodeAdd:
			result += x
		case OpCodeSub:
			result -= x
		case OpCodeMul:
			result *= x
		case OpCodeDiv:
			if x == 0 {
				return InstructionResult{}, errors.New("The divisor cannot be zero")
			}
			result /= x
		default:
			return InstructionResult{}, errors.Errorf("not supported math operator %T", op)
		}
	}

	return NewRegisterResult(result), nil
}

func ExecuteMoveInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	v, err := op.OperandValue(ctx)
	if err != nil {
		return InstructionResult{},
			errors.Wrapf(err, "Get move instruction operands, %v", op.ID.String())
	}
	return NewRegisterResult(v), nil
}

func ExecuteInputInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	v, err := op.OperandValue(ctx)
	if err != nil {
		return InstructionResult{},
			errors.Wrapf(err, "Get in instruction operands, %v", op.ID.String())
	}
	return NewRegisterResult(v), nil
}

var ReadWriteInstructions = map[OpCode]bool{
	OpCodeIn:   true,
	OpCodeMove: true,
}

func IsReadWriteInstruction(v Instruction) bool {
	return ReadWriteInstructions[v.OpCode]
}
