package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
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
	root := struct {
		Type   string
		Result json.RawMessage
	}{}

	err = json.Unmarshal(data, &root)
	if err != nil {
		return
	}

	var (
		emptyResult    = EmptyResult{}
		registerResult = RegisterResult{}
	)

	switch root.Type {
	case emptyResult.Type():
		err = json.Unmarshal(root.Result, &emptyResult)
		*v = InstructionResult{Type: root.Type, Result: emptyResult}
	case registerResult.Type():
		err = json.Unmarshal(root.Result, &registerResult)
		*v = InstructionResult{Type: root.Type, Result: registerResult}
	default:
		return errors.Errorf("Unmarshal Operand for type %s", root.Type)
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
