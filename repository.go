package gotlin

import (
	"context"
	"encoding/json"
	gerrors "errors"
	"sync"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var (
	_ SchedulerRepository   = (*SchedulerDBRepository)(nil)
	_ ProgramRepository     = (*ProgramDBRepository)(nil)
	_ InstructionRepository = (*InstructionDBRepository)(nil)
	_ ExecutorRepository    = (*ExecutorDBRepository)(nil)
)

var (
	_ SchedulerRepository   = (*SchedulerMemoryRepository)(nil)
	_ ProgramRepository     = (*ProgramMemoryRepository)(nil)
	_ InstructionRepository = (*InstructionMemoryRepository)(nil)
	_ ExecutorRepository    = (*ExecutorMemoryRepository)(nil)
)

var (
	InstructionTableName = "instructions"
	ProgramTableName     = "programs"
	SchedulerTableName   = "schedulers"
	ExecutorTableName    = "executors"
)

type InstructionRepository interface {
	Find(context.Context, InstructionID) (Instruction, error)
	Save(context.Context, *Instruction) error
}

type ProgramRepository interface {
	Find(context.Context, ProgramID) (Program, error)
	Save(context.Context, *Program) error
}

type SchedulerRepository interface {
	Find(context.Context, SchedulerID) (Scheduler, error)
	Save(context.Context, *Scheduler) error
}

type ExecutorRepository interface {
	Find(context.Context, ExecutorID) (Executor, error)
	Save(context.Context, *Executor) error
}

func requestNewUpdateTime(old int64) int64 {
	val := NewTimestamp().Value()
	if val <= old {
		val = old + 1
	}
	return val
}

func isRecordNotFound(err error) bool {
	return err != nil && (errors.Cause(err) == ErrNotFound || gerrors.Is(err, gorm.ErrRecordNotFound))
}

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
		return Scheduler{}, errors.Wrap(ErrNotFound, err.Error())
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
		return Program{}, errors.Wrap(ErrNotFound, err.Error())
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
		return Instruction{}, errors.Wrap(ErrNotFound, err.Error())
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
	err = r.db.WithContext(ctx).Create(&e).Error
	return
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
		return Executor{}, errors.Wrap(ErrNotFound, err.Error())
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

type SchedulerMemoryRepository struct {
	m  map[SchedulerID]Scheduler
	mu *sync.RWMutex
}

func NewSchedulerMemoryRepository() SchedulerMemoryRepository {
	return SchedulerMemoryRepository{
		m:  make(map[SchedulerID]Scheduler),
		mu: &sync.RWMutex{},
	}
}

func (r SchedulerMemoryRepository) Find(ctx context.Context, id SchedulerID) (Scheduler, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Scheduler{}, ErrNotFound
	}
	return m, nil
}

func (r SchedulerMemoryRepository) Save(ctx context.Context, m *Scheduler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		return errors.Errorf("Update Scheduler %s conflicts at version %d",
			m.ID, m.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type ProgramMemoryRepository struct {
	m  map[ProgramID]Program
	mu *sync.RWMutex
}

func NewProgramMemoryRepository() ProgramMemoryRepository {
	return ProgramMemoryRepository{
		m:  make(map[ProgramID]Program),
		mu: &sync.RWMutex{},
	}
}

func (r ProgramMemoryRepository) Find(ctx context.Context, id ProgramID) (Program, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Program{}, ErrNotFound
	}
	return m, nil
}

func (r ProgramMemoryRepository) Save(ctx context.Context, m *Program) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		return errors.Errorf("Update Program %s conflicts at version %d",
			m.ID, m.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type InstructionMemoryRepository struct {
	m  map[InstructionID]Instruction
	mu *sync.RWMutex
}

func NewInstructionMemoryRepository() InstructionMemoryRepository {
	return InstructionMemoryRepository{
		m:  make(map[InstructionID]Instruction),
		mu: &sync.RWMutex{},
	}
}

func (r InstructionMemoryRepository) Find(ctx context.Context, id InstructionID) (Instruction, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Instruction{}, ErrNotFound
	}
	return m, nil
}

func (r InstructionMemoryRepository) Save(ctx context.Context, m *Instruction) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		return errors.Errorf("Update Instruction %s conflicts at version %d",
			m.ID, m.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type ExecutorMemoryRepository struct {
	m  map[ExecutorID]Executor
	mu *sync.RWMutex
}

func NewExecutorMemoryRepository() ExecutorMemoryRepository {
	return ExecutorMemoryRepository{
		m:  make(map[ExecutorID]Executor),
		mu: &sync.RWMutex{},
	}
}

func (r ExecutorMemoryRepository) Find(ctx context.Context, id ExecutorID) (Executor, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Executor{}, ErrNotFound
	}
	return m, nil
}

func (r ExecutorMemoryRepository) Save(ctx context.Context, m *Executor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		return errors.Errorf("Update Executor %s conflicts at version %d",
			m.ID, m.UpdateTime)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}
