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
	IntersectInstructionHandler = InstructionHandler{
		OpCode:   OpCodeIntersect,
		Executor: ExecuteCollectionInstruction,
	}
	UnionInstructionHandler = InstructionHandler{
		OpCode:   OpCodeUnion,
		Executor: ExecuteCollectionInstruction,
	}
	DiffInstructionHandler = InstructionHandler{
		OpCode:   OpCodeDiff,
		Executor: ExecuteCollectionInstruction,
	}
)

var DefaultInstructionHandlers = []InstructionHandler{
	MoveInstructionHandler,
	InputInstructionHandler,
	AddInstructionHandler,
	SubInstructionHandler,
	MulInstructionHandler,
	DivInstructionHandler,
	IntersectInstructionHandler,
	UnionInstructionHandler,
	DiffInstructionHandler,
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

type collectionValues struct {
	lists   []interface{}
	strings [][]string
	ints    [][]int64
	floats  [][]float64
}

func getCollectionValues(list []interface{}) (collectionValues, bool) {
	strings := [][]string{}
	ints := [][]int64{}
	floats := [][]float64{}

	for _, v := range list {
		switch v2 := v.(type) {
		case []string:
			strings = append(strings, v2)
		case []int64:
			ints = append(ints, v2)
		case []float64:
			floats = append(floats, v2)
		case []interface{}:
			if len(v2) == 0 {
				return collectionValues{}, false
			}
			switch v2[0].(type) {
			case string:
				t := make([]string, 0, len(v2))
				for _, v3 := range v2 {
					t = append(t, v3.(string))
				}
				strings = append(strings, t)
			case int64:
				t := make([]int64, 0, len(v2))
				for _, v3 := range v2 {
					t = append(t, v3.(int64))
				}
				ints = append(ints, t)
			case float64:
				t := make([]float64, 0, len(v2))
				for _, v3 := range v2 {
					t = append(t, v3.(float64))
				}
				floats = append(floats, t)
			}
		default:
			return collectionValues{}, false
		}
	}

	nl, ns, ni, nf := len(list), len(strings), len(ints), len(floats)
	ok := nl == ns || nl == ni || nl == nf
	if !ok {
		return collectionValues{}, false
	}

	return collectionValues{list, strings, ints, floats}, true
}

func collectionValuesToInstructionResult(cv collectionValues, op OpCode) (InstructionResult, bool) {
	fn := func(v interface{}) InstructionResult {
		return NewRegisterResult(v)
	}

	strings, ints, floats := cv.strings, cv.ints, cv.floats

	switch {
	case len(strings) > 0:
		switch op {
		case OpCodeIntersect:
			return fn(StringsIntersect(strings)), true
		case OpCodeUnion:
			return fn(StringsUnion(strings)), true
		case OpCodeDiff:
			return fn(StringsDiff(strings)), true
		}
	case len(ints) > 0:
		switch op {
		case OpCodeIntersect:
			return fn(IntsIntersect(ints)), true
		case OpCodeUnion:
			return fn(IntsUnion(ints)), true
		case OpCodeDiff:
			return fn(IntsDiff(ints)), true
		}
	case len(floats) > 0:
		switch op {
		case OpCodeIntersect:
			return fn(FloatsIntersect(floats)), true
		case OpCodeUnion:
			return fn(FloatsUnion(floats)), true
		case OpCodeDiff:
			return fn(FloatsDiff(floats)), true
		}
	}

	return InstructionResult{}, false
}

func ExecuteCollectionInstruction(ctx context.Context, op Instruction, args ...Instruction) (InstructionResult, error) {
	argc := len(args)
	ok := (op.OpCode == OpCodeIntersect || op.OpCode == OpCodeUnion || op.OpCode == OpCodeDiff) && argc > 0
	if !ok {
		return InstructionResult{},
			errors.Errorf("Empty collection instruction operand, %v, opcode %s, argc %d",
				op.ID.String(), op.OpCode, argc)
	}

	list := []interface{}{}
	for _, in := range args {
		tmp, err := in.InstructionResult(ctx)
		if err != nil {
			return InstructionResult{},
				errors.Wrapf(err, "Get collection instruction operands, %v", in.ID.String())
		}
		list = append(list, tmp)
	}

	cv, ok := getCollectionValues(list)
	if !ok {
		return InstructionResult{},
			errors.Errorf("Collection Operands only support []string/[]int64/[]float64, should be %d %T, strings %d ints %d floats %d",
				len(list), list[0], len(cv.strings), len(cv.ints), len(cv.floats))
	}

	res, ok := collectionValuesToInstructionResult(cv, op.OpCode)
	if ok {
		return res, nil
	}

	return InstructionResult{},
		errors.Errorf("Collection Operands only support []string/[]int64/[]float64, should be %d %T, %s %s",
			len(list), list[0], op.ID.String(), op.OpCode)
}

var ReadWriteInstructions = map[OpCode]bool{
	OpCodeIn:   true,
	OpCodeMove: true,
}

func IsReadWriteInstruction(v Instruction) bool {
	return ReadWriteInstructions[v.OpCode]
}

func StringsIntersect(ll [][]string) []string {
	n := len(ll)
	min := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) < len(ll[min]) {
			min = i
		}
	}

	c := ll[min]

	f := make(map[string]int)
	for _, v := range c {
		f[v] = 0
	}

	for _, v := range ll {
		for _, v2 := range v {
			_, ok := f[v2]
			if ok {
				f[v2]++
			}
		}
	}

	l := make([]string, 0, len(c))
	for _, v := range c {
		if f[v] == n {
			l = append(l, v)
		}
	}

	return l
}

func IntsIntersect(ll [][]int64) []int64 {
	n := len(ll)
	min := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) < len(ll[min]) {
			min = i
		}
	}

	c := ll[min]

	f := make(map[int64]int)
	for _, v := range c {
		f[v] = 0
	}

	for _, v := range ll {
		for _, v2 := range v {
			_, ok := f[v2]
			if ok {
				f[v2]++
			}
		}
	}

	l := make([]int64, 0, len(c))
	for _, v := range c {
		if f[v] == n {
			l = append(l, v)
		}
	}

	return l
}

func FloatsIntersect(ll [][]float64) []float64 {
	n := len(ll)
	min := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) < len(ll[min]) {
			min = i
		}
	}

	c := ll[min]

	f := make(map[float64]int)
	for _, v := range c {
		f[v] = 0
	}

	for _, v := range ll {
		for _, v2 := range v {
			_, ok := f[v2]
			if ok {
				f[v2]++
			}
		}
	}

	l := make([]float64, 0, len(c))
	for _, v := range c {
		if f[v] == n {
			l = append(l, v)
		}
	}

	return l
}

func StringsUnion(ll [][]string) []string {
	n := len(ll)
	max := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) >= len(ll[max]) {
			max = i
		}
	}

	f := make(map[string]bool)
	l := make([]string, 0, len(ll[max]))

	for _, v := range ll {
		for _, v2 := range v {
			if !f[v2] {
				f[v2] = true
				l = append(l, v2)
			}
		}
	}

	return l
}

func IntsUnion(ll [][]int64) []int64 {
	n := len(ll)
	max := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) >= len(ll[max]) {
			max = i
		}
	}

	f := make(map[int64]bool)
	l := make([]int64, 0, len(ll[max]))

	for _, v := range ll {
		for _, v2 := range v {
			if !f[v2] {
				f[v2] = true
				l = append(l, v2)
			}
		}
	}

	return l
}

func FloatsUnion(ll [][]float64) []float64 {
	n := len(ll)
	max := 0
	for i := 1; i < n; i++ {
		if len(ll[i]) >= len(ll[max]) {
			max = i
		}
	}

	f := make(map[float64]bool)
	l := make([]float64, 0, len(ll[max]))

	for _, v := range ll {
		for _, v2 := range v {
			if !f[v2] {
				f[v2] = true
				l = append(l, v2)
			}
		}
	}

	return l
}

func StringsDiff(ll [][]string) []string {
	n := len(ll)
	if n == 0 {
		return []string{}
	}

	c := ll[0]

	f := make(map[string]bool)
	for _, v := range c {
		f[v] = true
	}

	for i, v := range ll {
		if i == 0 {
			continue
		}
		for _, v2 := range v {
			if f[v2] {
				f[v2] = false
			}
		}
	}

	l := make([]string, 0, len(c))
	for _, v := range c {
		if f[v] {
			l = append(l, v)
		}
	}

	return l
}

func IntsDiff(ll [][]int64) []int64 {
	n := len(ll)
	if n == 0 {
		return []int64{}
	}

	c := ll[0]

	f := make(map[int64]bool)
	for _, v := range c {
		f[v] = true
	}

	for i, v := range ll {
		if i == 0 {
			continue
		}
		for _, v2 := range v {
			if f[v2] {
				f[v2] = false
			}
		}
	}

	l := make([]int64, 0, len(c))
	for _, v := range c {
		if f[v] {
			l = append(l, v)
		}
	}

	return l
}

func FloatsDiff(ll [][]float64) []float64 {
	n := len(ll)
	if n == 0 {
		return []float64{}
	}

	c := ll[0]

	f := make(map[float64]bool)
	for _, v := range c {
		f[v] = true
	}

	for i, v := range ll {
		if i == 0 {
			continue
		}
		for _, v2 := range v {
			if f[v2] {
				f[v2] = false
			}
		}
	}

	l := make([]float64, 0, len(c))
	for _, v := range c {
		if f[v] {
			l = append(l, v)
		}
	}

	return l
}
