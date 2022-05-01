package gotlin

import (
	"context"
	gerrors "errors"

	"gorm.io/gorm"
)

var (
	InstructionTableName = "instructions"
	ProgramTableName     = "programs"
	SchedulerTableName   = "schedulers"
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

func requestNewUpdateTime(old int64) int64 {
	val := NewTimestamp().Value()
	if val <= old {
		val = old + 1
	}
	return val
}

func isRecordNotFound(err error) bool {
	return gerrors.Is(err, gorm.ErrRecordNotFound)
}
