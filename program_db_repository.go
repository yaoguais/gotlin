package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var _ ProgramRepository = (*ProgramDBRepository)(nil)

type ProgramEntity struct {
	ID         string `gorm:"primaryKey"`
	Code       string
	State      string
	Error      string
	Processor  string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (ProgramEntity) TableName() string {
	return ProgramTableName
}

type ProgramDBRepository struct {
	db *gorm.DB
}

func NewProgramDBRepository(db *gorm.DB) ProgramDBRepository {
	return ProgramDBRepository{db: db}
}

func (r ProgramDBRepository) Find(ctx context.Context, id ProgramID) (m Program, err error) {
	e := ProgramEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return
	}
	return ProgramEntityConverter{}.ToModel(e)
}

func (r ProgramDBRepository) Save(ctx context.Context, m *Program) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		return r.create(ctx, m)
	} else if err != nil {
		return err
	}
	return r.update(ctx, m)
}

func (r ProgramDBRepository) create(ctx context.Context, m *Program) (err error) {
	e, err := ProgramEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r ProgramDBRepository) update(ctx context.Context, m *Program) (err error) {
	e, err := ProgramEntityConverter{}.ToEntity(m)
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
		return errors.Errorf("Update program %s conflicts at version %d",
			e.ID, e.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type ProgramEntityConverter struct {
}

func (c ProgramEntityConverter) ToEntity(m *Program) (e ProgramEntity, err error) {
	code, err := json.Marshal(m.Code)
	if err != nil {
		return
	}

	error := ""
	if m.Error != nil {
		error = m.Error.Error()
	}

	processor, err := json.Marshal(m.Processor)
	if err != nil {
		return
	}

	return ProgramEntity{
		ID:         m.ID.String(),
		Code:       string(code),
		State:      string(m.State),
		Error:      error,
		Processor:  string(processor),
		CreateTime: m.CreateTime.Value(),
		UpdateTime: m.UpdateTime.Value(),
		FinishTime: m.FinishTime.Value(),
	}, nil
}

func (c ProgramEntityConverter) ToModel(e ProgramEntity) (m Program, err error) {
	id, err := ParseProgramID(e.ID)
	if err != nil {
		return
	}

	var code ProgramCode
	err = json.Unmarshal([]byte(e.Code), &code)
	if err != nil {
		return
	}

	var processor ProcessorContext
	err = json.Unmarshal([]byte(e.Processor), &processor)
	if err != nil {
		return
	}

	var error error
	if e.Error != "" {
		error = errors.New(e.Error)
	}

	return Program{
		ID:         id,
		Code:       code,
		State:      State(e.State),
		Error:      error,
		Processor:  processor,
		CreateTime: ParseTimestamp(e.CreateTime),
		UpdateTime: ParseTimestamp(e.UpdateTime),
		FinishTime: ParseTimestamp(e.FinishTime),
	}, nil
}
