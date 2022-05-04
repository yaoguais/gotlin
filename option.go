package gotlin

import "gorm.io/gorm"
import "google.golang.org/grpc"

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
		g.ServerExecutor = enable
	}
}

func WithEnableServer(enable bool) Option {
	return func(g *Gotlin) {
		g.EnableServer = enable
	}
}

func WithServerAddress(addr string) Option {
	return func(g *Gotlin) {
		g.ServerAddress = addr
	}
}

func WithGRPCServerOption(options ...grpc.ServerOption) Option {
	return func(g *Gotlin) {
		g.GRPCOption = options
	}
}
