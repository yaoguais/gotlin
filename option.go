package gotlin

import "gorm.io/gorm"

type Option func(g *Gotlin)

func WithDatabase(db *gorm.DB) Option {
	return func(g *Gotlin) {
		g.SchedulerRepository = NewSchedulerDBRepository(db)
		g.ProgramRepository = NewProgramDBRepository(db)
		g.InstructionRepository = NewInstructionDBRepository(db)
		g.ExecutorRepository = NewExecutorDBRepository(db)
	}
}

func WithServerExecutor(enable bool) Option {
	return func(g *Gotlin) {
		g.ServerExecutor = true
	}
}
