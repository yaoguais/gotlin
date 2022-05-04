package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var _ ExecutorRepository = (*ExecutorDBRepository)(nil)

type ExecutorEntity struct {
	ID         string `gorm:"primaryKey"`
	Labels     string
	Host       string
	State      string
	Error      string
	Limits     string
	Usages     string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (ExecutorEntity) TableName() string {
	return ExecutorTableName
}

type ExecutorDBRepository struct {
	db *gorm.DB
}

func NewExecutorDBRepository(db *gorm.DB) ExecutorDBRepository {
	return ExecutorDBRepository{db: db}
}

func (r ExecutorDBRepository) Find(ctx context.Context, id ExecutorID) (m Executor, err error) {
	e := ExecutorEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return
	}
	return ExecutorEntityConverter{}.ToModel(e)
}

func (r ExecutorDBRepository) Save(ctx context.Context, m *Executor) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		return r.create(ctx, m)
	} else if err != nil {
		return err
	}
	return r.update(ctx, m)
}

func (r ExecutorDBRepository) create(ctx context.Context, m *Executor) (err error) {
	e, err := ExecutorEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r ExecutorDBRepository) update(ctx context.Context, m *Executor) (err error) {
	e, err := ExecutorEntityConverter{}.ToEntity(m)
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
		return errors.Errorf("Update executor %s conflicts at version %d",
			e.ID, e.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type ExecutorEntityConverter struct {
}

func (c ExecutorEntityConverter) ToEntity(m *Executor) (e ExecutorEntity, err error) {
	labels, err := json.Marshal(m.Labels)
	if err != nil {
		return
	}

	error := ""
	if m.Error != nil {
		error = m.Error.Error()
	}

	limits, err := json.Marshal(m.Limits)
	if err != nil {
		return
	}

	usages, err := json.Marshal(m.Usages)
	if err != nil {
		return
	}

	return ExecutorEntity{
		ID:         m.ID.String(),
		Labels:     string(labels),
		Host:       m.Host.String(),
		State:      string(m.State),
		Error:      error,
		Limits:     string(limits),
		Usages:     string(usages),
		CreateTime: m.CreateTime.Value(),
		UpdateTime: m.UpdateTime.Value(),
		FinishTime: m.FinishTime.Value(),
	}, nil
}

func (c ExecutorEntityConverter) ToModel(e ExecutorEntity) (m Executor, err error) {
	id, err := ParseExecutorID(e.ID)
	if err != nil {
		return
	}

	var labels Labels
	err = json.Unmarshal([]byte(e.Labels), &labels)
	if err != nil {
		return
	}

	var error error
	if e.Error != "" {
		error = errors.New(e.Error)
	}

	var limits Resource
	err = json.Unmarshal([]byte(e.Limits), &limits)
	if err != nil {
		return
	}

	var usages Resource
	err = json.Unmarshal([]byte(e.Usages), &usages)
	if err != nil {
		return
	}

	return Executor{
		ID:         id,
		Labels:     labels,
		Host:       Host(e.Host),
		State:      State(e.State),
		Error:      error,
		Limits:     limits,
		Usages:     usages,
		CreateTime: ParseTimestamp(e.CreateTime),
		UpdateTime: ParseTimestamp(e.UpdateTime),
		FinishTime: ParseTimestamp(e.FinishTime),
	}, nil
}
