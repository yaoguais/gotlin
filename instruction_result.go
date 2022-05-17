package gotlin

import (
	"context"
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

type InstructionResult struct {
	Type   string
	Result InstructionResulter
}

func NewEmptyInstructionResult() InstructionResult {
	v := EmptyResult{}
	return InstructionResult{Type: v.Type(), Result: v}
}

func NewRegisterResult(u interface{}) InstructionResult {
	v := RegisterResult{Value: u}
	return InstructionResult{Type: v.Type(), Result: v}
}

func (v InstructionResult) InstructionResult(ctx context.Context) (interface{}, error) {
	return v.Result.InstructionResult(ctx)
}

func (v InstructionResult) RegisterValue() (interface{}, bool) {
	u, ok := v.Result.(RegisterResult)
	if !ok {
		return nil, false
	}
	return u.Value, true
}

func (v *InstructionResult) UnmarshalJSON(data []byte) (err error) {
	return v.unmarshalJSON(data, json.Unmarshal)
}

func (v *InstructionResult) unmarshalJSON(data []byte, f Unmarshal) (err error) {
	root := struct {
		Type   string
		Result json.RawMessage
	}{}

	err = f(data, &root)
	if err != nil {
		return
	}

	var (
		emptyResult    = EmptyResult{}
		registerResult = RegisterResult{}
	)

	switch root.Type {
	case emptyResult.Type():
		err = f(root.Result, &emptyResult)
		*v = InstructionResult{Type: root.Type, Result: emptyResult}
	case registerResult.Type():
		err = f(root.Result, &registerResult)
		*v = InstructionResult{Type: root.Type, Result: registerResult}
	default:
		return newErrorf("Unmarshal Operand for type %s", root.Type)
	}
	return
}

func (v *InstructionResult) UnmarshalMsgpack(data []byte) error {
	return v.unmarshalMsgpack(data, msgpack.Unmarshal)
}

func (v *InstructionResult) unmarshalMsgpack(data []byte, f Unmarshal) (err error) {
	root := struct {
		Type   string
		Result msgpack.RawMessage
	}{}

	err = f(data, &root)
	if err != nil {
		return
	}

	var (
		emptyResult    = EmptyResult{}
		registerResult = RegisterResult{}
	)

	switch root.Type {
	case emptyResult.Type():
		err = f(root.Result, &emptyResult)
		*v = InstructionResult{Type: root.Type, Result: emptyResult}
	case registerResult.Type():
		err = f(root.Result, &registerResult)
		*v = InstructionResult{Type: root.Type, Result: registerResult}
	default:
		return newErrorf("Unmarshal Operand for type %s", root.Type)
	}
	return
}

type InstructionResulter interface {
	Type() string
	InstructionResult(context.Context) (interface{}, error)
}

type EmptyResult struct {
}

func (EmptyResult) Type() string {
	return "Empty"
}

func (EmptyResult) InstructionResult(context.Context) (interface{}, error) {
	return 0, nil
}

type RegisterResult struct {
	Value interface{}
}

func (RegisterResult) Type() string {
	return "Register"
}

func (v RegisterResult) InstructionResult(context.Context) (interface{}, error) {
	return v.Value, nil
}
