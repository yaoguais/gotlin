package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
)

type Operand struct {
	Type  string
	Value OperandValuer
}

func NewEmptyOperand() Operand {
	v := EmptyInput{}
	return Operand{Type: v.Type(), Value: v}
}

func NewImmediateValue(u interface{}) Operand {
	v := Immediate{Value: u}
	return Operand{Type: v.Type(), Value: v}
}

func (v Operand) OperandValue(ctx context.Context) (interface{}, error) {
	return v.Value.OperandValue(ctx)
}

func (v *Operand) ImmediateValue() (interface{}, bool) {
	u, ok := v.Value.(Immediate)
	if !ok {
		return nil, false
	}
	return u.Value, true
}

func (v *Operand) UnmarshalJSON(data []byte) (err error) {
	root := struct {
		Type  string
		Value json.RawMessage
	}{}

	err = json.Unmarshal(data, &root)
	if err != nil {
		return
	}

	var (
		emptyInput    = EmptyInput{}
		immediate     = Immediate{}
		databaseQuery = DatabaseQuery{}
	)

	switch root.Type {
	case emptyInput.Type():
		err = json.Unmarshal(root.Value, &emptyInput)
		*v = Operand{Type: root.Type, Value: emptyInput}
	case immediate.Type():
		err = json.Unmarshal(root.Value, &immediate)
		*v = Operand{Type: root.Type, Value: immediate}
	case databaseQuery.Type():
		err = json.Unmarshal(root.Value, &databaseQuery)
		*v = Operand{Type: root.Type, Value: databaseQuery}
	default:
		return errors.Errorf("Unmarshal Operand for type %s", root.Type)
	}
	return
}

type OperandValuer interface {
	Type() string
	OperandValue(context.Context) (interface{}, error)
}

var _ OperandValuer = (*EmptyInput)(nil)

type EmptyInput struct {
}

func (EmptyInput) Type() string {
	return "Empty"
}

func (EmptyInput) OperandValue(context.Context) (interface{}, error) {
	return 0, nil
}

type Immediate struct {
	Value interface{}
}

func (Immediate) Type() string {
	return "Immediate"
}

func (v Immediate) OperandValue(context.Context) (interface{}, error) {
	return v.Value, nil
}

type DatabaseQuery struct {
	Driver     string
	Dsn        string
	Query      string
	Converters []string
}

func (v DatabaseQuery) Type() string {
	return "Database"
}

func (v DatabaseQuery) OperandValue(context.Context) (interface{}, error) {
	return 0, nil
}
