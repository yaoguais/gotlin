package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var _ SchedulerRepository = (*SchedulerDBRepository)(nil)

type SchedulerEntity struct {
	ID         string `gorm:"primaryKey"`
	Programs   string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (SchedulerEntity) TableName() string {
	return SchedulerTableName
}

type SchedulerDBRepository struct {
	db *gorm.DB
}

func NewSchedulerDBRepository(db *gorm.DB) SchedulerDBRepository {
	return SchedulerDBRepository{db: db}
}

func (r SchedulerDBRepository) Find(ctx context.Context, id SchedulerID) (m Scheduler, err error) {
	e := SchedulerEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return
	}
	return SchedulerEntityConverter{}.ToModel(e)
}

func (r SchedulerDBRepository) Save(ctx context.Context, m *Scheduler) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		return r.create(ctx, m)
	} else if err != nil {
		return err
	}
	return r.update(ctx, m)
}

func (r SchedulerDBRepository) create(ctx context.Context, m *Scheduler) (err error) {
	e, err := SchedulerEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r SchedulerDBRepository) update(ctx context.Context, m *Scheduler) (err error) {
	e, err := SchedulerEntityConverter{}.ToEntity(m)
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
		return errors.Errorf("Update scheduler %s conflicts at version %d",
			e.ID, e.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type SchedulerEntityConverter struct {
}

func (c SchedulerEntityConverter) ToEntity(m *Scheduler) (e SchedulerEntity, err error) {
	programs, err := json.Marshal(m.Programs)
	if err != nil {
		return
	}
	return SchedulerEntity{
		ID:         m.ID.String(),
		Programs:   string(programs),
		CreateTime: m.CreateTime.Value(),
		UpdateTime: m.UpdateTime.Value(),
		FinishTime: m.FinishTime.Value(),
	}, nil
}

func (c SchedulerEntityConverter) ToModel(e SchedulerEntity) (m Scheduler, err error) {
	id, err := ParseSchedulerID(e.ID)
	if err != nil {
		return
	}

	var programs ScheduledPrograms
	err = json.Unmarshal([]byte(e.Programs), &programs)
	if err != nil {
		return
	}

	return Scheduler{
		ID:         id,
		Programs:   programs,
		CreateTime: ParseTimestamp(e.CreateTime),
		UpdateTime: ParseTimestamp(e.UpdateTime),
		FinishTime: ParseTimestamp(e.FinishTime),
	}, nil
}
