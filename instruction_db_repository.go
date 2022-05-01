package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var _ InstructionRepository = (*InstructionDBRepository)(nil)

type InstructionEntity struct {
	ID         string `gorm:"primaryKey"`
	OpCode     string `gorm:"column:opcode"`
	Operand    string
	Result     string
	State      string
	Error      string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (InstructionEntity) TableName() string {
	return InstructionTableName
}

type InstructionDBRepository struct {
	db *gorm.DB
}

func NewInstructionDBRepository(db *gorm.DB) InstructionDBRepository {
	return InstructionDBRepository{db: db}
}

func (r InstructionDBRepository) Find(ctx context.Context, id InstructionID) (m Instruction, err error) {
	e := InstructionEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return
	}
	return InstructionEntityConverter{}.ToModel(e)
}

func (r InstructionDBRepository) Save(ctx context.Context, m *Instruction) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		return r.create(ctx, m)
	} else if err != nil {
		return err
	}
	return r.update(ctx, m)
}

func (r InstructionDBRepository) create(ctx context.Context, m *Instruction) (err error) {
	e, err := InstructionEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r InstructionDBRepository) update(ctx context.Context, m *Instruction) (err error) {
	e, err := InstructionEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}

	oldUpdateTime := e.UpdateTime
	e.UpdateTime = requestNewUpdateTime(e.UpdateTime)

	res := r.db.WithContext(ctx).
		Model(&e).
		Select("*").
		Where("id = ?", e.ID).
		Where("update_time = ?", oldUpdateTime).
		Updates(e)

	if err = res.Error; err != nil {
		return
	}

	if res.RowsAffected == 0 {
		return errors.Errorf("Update instruction %s conflicts at version %d",
			e.ID, e.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type InstructionEntityConverter struct {
}

func (c InstructionEntityConverter) ToEntity(m *Instruction) (e InstructionEntity, err error) {
	operand, err := json.Marshal(m.Operand)
	if err != nil {
		return
	}

	result, err := json.Marshal(m.Result)
	if err != nil {
		return
	}

	error := ""
	if m.Error != nil {
		error = m.Error.Error()
	}

	return InstructionEntity{
		ID:         m.ID.String(),
		OpCode:     string(m.OpCode),
		Operand:    string(operand),
		Result:     string(result),
		State:      string(m.State),
		Error:      error,
		CreateTime: m.CreateTime.Value(),
		UpdateTime: m.UpdateTime.Value(),
		FinishTime: m.FinishTime.Value(),
	}, nil
}

func (c InstructionEntityConverter) ToModel(e InstructionEntity) (m Instruction, err error) {
	id, err := ParseInstructionID(e.ID)
	if err != nil {
		return
	}

	var operand Operand
	err = json.Unmarshal([]byte(e.Operand), &operand)
	if err != nil {
		return
	}

	var result InstructionResult
	err = json.Unmarshal([]byte(e.Result), &result)
	if err != nil {
		return
	}

	var error error
	if e.Error != "" {
		error = errors.New(e.Error)
	}

	return Instruction{
		ID:         id,
		OpCode:     OpCode(e.OpCode),
		Operand:    operand,
		Result:     result,
		State:      State(e.State),
		Error:      error,
		CreateTime: ParseTimestamp(e.CreateTime),
		UpdateTime: ParseTimestamp(e.UpdateTime),
		FinishTime: ParseTimestamp(e.FinishTime),
	}, nil
}
