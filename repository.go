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
	_ SchedulerRepository   = (*schedulerDBRepository)(nil)
	_ ProgramRepository     = (*programDBRepository)(nil)
	_ InstructionRepository = (*instructionDBRepository)(nil)
	_ ExecutorRepository    = (*executorDBRepository)(nil)
)

var (
	_ SchedulerRepository   = (*schedulerMemoryRepository)(nil)
	_ ProgramRepository     = (*programMemoryRepository)(nil)
	_ InstructionRepository = (*instructionMemoryRepository)(nil)
	_ ExecutorRepository    = (*executorMemoryRepository)(nil)
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

type schedulerEntity struct {
	ID         string `gorm:"primaryKey"`
	Programs   string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (schedulerEntity) TableName() string {
	return SchedulerTableName
}

type schedulerDBRepository struct {
	db *gorm.DB
	formatError
}

func newSchedulerDBRepository(db *gorm.DB) schedulerDBRepository {
	e := formatError{format: "Scheduler %s"}
	return schedulerDBRepository{db, e}
}

func (r schedulerDBRepository) Find(ctx context.Context, id SchedulerID) (m Scheduler, err error) {
	e := schedulerEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return Scheduler{}, r.error(ErrNotFound, err, id)
	}
	m, err = schedulerEntityConverter{}.ToModel(e)
	return m, r.error(ErrFind, err, id)
}

func (r schedulerDBRepository) Save(ctx context.Context, m *Scheduler) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		err = r.create(ctx, m)
		return r.error(ErrPersistent, err, m.ID)
	} else if err != nil {
		return err
	}
	err = r.update(ctx, m)
	return r.error(ErrPersistent, err, m.ID)
}

func (r schedulerDBRepository) create(ctx context.Context, m *Scheduler) (err error) {
	e, err := schedulerEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r schedulerDBRepository) update(ctx context.Context, m *Scheduler) (err error) {
	e, err := schedulerEntityConverter{}.ToEntity(m)
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
		err = newErrorf("at version %d", e.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type schedulerEntityConverter struct {
}

func (c schedulerEntityConverter) ToEntity(m *Scheduler) (e schedulerEntity, err error) {
	programs, err := json.Marshal(m.Programs)
	if err != nil {
		return
	}
	return schedulerEntity{
		ID:         m.ID.String(),
		Programs:   string(programs),
		CreateTime: m.CreateTime.Value(),
		UpdateTime: m.UpdateTime.Value(),
		FinishTime: m.FinishTime.Value(),
	}, nil
}

func (c schedulerEntityConverter) ToModel(e schedulerEntity) (m Scheduler, err error) {
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

type programEntity struct {
	ID         string `gorm:"primaryKey"`
	Code       string
	State      string
	Error      string
	Processor  string
	CreateTime int64
	UpdateTime int64
	FinishTime int64
}

func (programEntity) TableName() string {
	return ProgramTableName
}

type programDBRepository struct {
	db *gorm.DB
	formatError
}

func newProgramDBRepository(db *gorm.DB) programDBRepository {
	e := formatError{format: "Program %s"}
	return programDBRepository{db, e}
}

func (r programDBRepository) Find(ctx context.Context, id ProgramID) (m Program, err error) {
	e := programEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return Program{}, r.error(ErrNotFound, err, id)
	}
	m, err = programEntityConverter{}.ToModel(e)
	return m, r.error(ErrFind, err, id)
}

func (r programDBRepository) Save(ctx context.Context, m *Program) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		err = r.create(ctx, m)
		return r.error(ErrPersistent, err, m.ID)
	} else if err != nil {
		return err
	}
	err = r.update(ctx, m)
	return r.error(ErrPersistent, err, m.ID)
}

func (r programDBRepository) create(ctx context.Context, m *Program) (err error) {
	e, err := programEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r programDBRepository) update(ctx context.Context, m *Program) (err error) {
	e, err := programEntityConverter{}.ToEntity(m)
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
		err = newErrorf("at version %d", e.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type programEntityConverter struct {
}

func (c programEntityConverter) ToEntity(m *Program) (e programEntity, err error) {
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

	return programEntity{
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

func (c programEntityConverter) ToModel(e programEntity) (m Program, err error) {
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
		error = newError(e.Error)
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

type instructionEntity struct {
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

func (instructionEntity) TableName() string {
	return InstructionTableName
}

type instructionDBRepository struct {
	db *gorm.DB
	formatError
}

func newInstructionDBRepository(db *gorm.DB) instructionDBRepository {
	e := formatError{format: "Instruction %s"}
	return instructionDBRepository{db, e}
}

func (r instructionDBRepository) Find(ctx context.Context, id InstructionID) (m Instruction, err error) {
	e := instructionEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return Instruction{}, r.error(ErrNotFound, err, id)
	}
	m, err = instructionEntityConverter{}.ToModel(e)
	return m, r.error(ErrFind, err, id)
}

func (r instructionDBRepository) Save(ctx context.Context, m *Instruction) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		err = r.create(ctx, m)
		return r.error(ErrPersistent, err, m.ID)
	} else if err != nil {
		return err
	}
	err = r.update(ctx, m)
	return r.error(ErrPersistent, err, m.ID)
}

func (r instructionDBRepository) create(ctx context.Context, m *Instruction) (err error) {
	e, err := instructionEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	err = r.db.WithContext(ctx).Create(&e).Error
	return
}

func (r instructionDBRepository) update(ctx context.Context, m *Instruction) (err error) {
	e, err := instructionEntityConverter{}.ToEntity(m)
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
		err = newErrorf("at version %d", e.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type instructionEntityConverter struct {
}

func (c instructionEntityConverter) ToEntity(m *Instruction) (e instructionEntity, err error) {
	operand, err := marshal(m.Operand)
	if err != nil {
		return
	}

	result, err := marshal(m.Result)
	if err != nil {
		return
	}

	error := ""
	if m.Error != nil {
		error = m.Error.Error()
	}

	return instructionEntity{
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

func (c instructionEntityConverter) ToModel(e instructionEntity) (m Instruction, err error) {
	id, err := ParseInstructionID(e.ID)
	if err != nil {
		return
	}

	var operand Operand
	err = unmarshal([]byte(e.Operand), &operand)
	if err != nil {
		return
	}

	var result InstructionResult
	err = unmarshal([]byte(e.Result), &result)
	if err != nil {
		return
	}

	var error error
	if e.Error != "" {
		error = newError(e.Error)
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

type executorEntity struct {
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

func (executorEntity) TableName() string {
	return ExecutorTableName
}

type executorDBRepository struct {
	db *gorm.DB
	formatError
}

func newExecutorDBRepository(db *gorm.DB) executorDBRepository {
	e := formatError{format: "Executor %s"}
	return executorDBRepository{db, e}
}

func (r executorDBRepository) Find(ctx context.Context, id ExecutorID) (m Executor, err error) {
	e := executorEntity{}
	err = r.db.WithContext(ctx).Where("id = ?", id.String()).First(&e).Error
	if err != nil {
		return Executor{}, r.error(ErrNotFound, err, m.ID)
	}
	m, err = executorEntityConverter{}.ToModel(e)
	return m, r.error(ErrFind, err, m.ID)
}

func (r executorDBRepository) Save(ctx context.Context, m *Executor) (err error) {
	_, err = r.Find(ctx, m.ID)
	if isRecordNotFound(err) {
		err = r.create(ctx, m)
		return r.error(ErrPersistent, err, m.ID)
	} else if err != nil {
		return err
	}
	err = r.update(ctx, m)
	return r.error(ErrPersistent, err, m.ID)
}

func (r executorDBRepository) create(ctx context.Context, m *Executor) (err error) {
	e, err := executorEntityConverter{}.ToEntity(m)
	if err != nil {
		return
	}
	return r.db.WithContext(ctx).Create(&e).Error
}

func (r executorDBRepository) update(ctx context.Context, m *Executor) (err error) {
	e, err := executorEntityConverter{}.ToEntity(m)
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
		err = newErrorf("at version %d", e.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(e.UpdateTime)

	return
}

type executorEntityConverter struct {
}

func (c executorEntityConverter) ToEntity(m *Executor) (e executorEntity, err error) {
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

	return executorEntity{
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

func (c executorEntityConverter) ToModel(e executorEntity) (m Executor, err error) {
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
		error = newError(e.Error)
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

type schedulerMemoryRepository struct {
	m  map[SchedulerID]Scheduler
	mu *sync.RWMutex
	formatError
}

func newSchedulerMemoryRepository() schedulerMemoryRepository {
	e := formatError{format: "Scheduler %s"}
	return schedulerMemoryRepository{
		m:           make(map[SchedulerID]Scheduler),
		mu:          &sync.RWMutex{},
		formatError: e,
	}
}

func (r schedulerMemoryRepository) Find(ctx context.Context, id SchedulerID) (Scheduler, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Scheduler{}, r.error(ErrNotFound, ErrUndoubted, id)
	}
	return m, nil
}

func (r schedulerMemoryRepository) Save(ctx context.Context, m *Scheduler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		err := newErrorf("at version %d", m.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type programMemoryRepository struct {
	m  map[ProgramID]Program
	mu *sync.RWMutex
	formatError
}

func newProgramMemoryRepository() programMemoryRepository {
	e := formatError{format: "Program %s"}
	return programMemoryRepository{
		m:           make(map[ProgramID]Program),
		mu:          &sync.RWMutex{},
		formatError: e,
	}
}

func (r programMemoryRepository) Find(ctx context.Context, id ProgramID) (Program, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Program{}, r.error(ErrNotFound, ErrUndoubted, id)
	}
	return m, nil
}

func (r programMemoryRepository) Save(ctx context.Context, m *Program) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		err := newErrorf("at version %d", m.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type instructionMemoryRepository struct {
	m  map[InstructionID]Instruction
	mu *sync.RWMutex
	formatError
}

func newInstructionMemoryRepository() instructionMemoryRepository {
	e := formatError{format: "Instruction %s"}
	return instructionMemoryRepository{
		m:           make(map[InstructionID]Instruction),
		mu:          &sync.RWMutex{},
		formatError: e,
	}
}

func (r instructionMemoryRepository) Find(ctx context.Context, id InstructionID) (Instruction, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Instruction{}, r.error(ErrNotFound, ErrUndoubted, id)
	}
	return m, nil
}

func (r instructionMemoryRepository) Save(ctx context.Context, m *Instruction) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		err := newErrorf("at version %d", m.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}

type executorMemoryRepository struct {
	m  map[ExecutorID]Executor
	mu *sync.RWMutex
	formatError
}

func newExecutorMemoryRepository() executorMemoryRepository {
	e := formatError{format: "Executor %s"}
	return executorMemoryRepository{
		m:           make(map[ExecutorID]Executor),
		mu:          &sync.RWMutex{},
		formatError: e,
	}
}

func (r executorMemoryRepository) Find(ctx context.Context, id ExecutorID) (Executor, error) {
	r.mu.RLock()
	m, ok := r.m[id]
	r.mu.RUnlock()
	if !ok {
		return Executor{}, r.error(ErrNotFound, ErrUndoubted, id)
	}
	return m, nil
}

func (r executorMemoryRepository) Save(ctx context.Context, m *Executor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.m[m.ID]
	if !ok {
		r.m[m.ID] = *m
		return nil
	}

	if !old.UpdateTime.IsEqual(m.UpdateTime) {
		err := newErrorf("at version %d", m.UpdateTime)
		return r.error(ErrConflict, err, m.ID)
	}

	m.UpdateTime = ParseTimestamp(requestNewUpdateTime(m.UpdateTime.Value()))
	r.m[m.ID] = *m

	return nil
}
