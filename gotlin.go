package gotlin

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"gorm.io/gorm"
)

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

type Gotlin struct {
	SchedulerRepository   SchedulerRepository
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
	ExecutorRepository    ExecutorRepository
	EnableServer          bool
	ServerAddress         string
	ServerExecutor        bool
	GRPCOption            []grpc.ServerOption

	executorPool  *ExecutorPool
	schedulerPool *SchedulerPool
	gs            *grpc.Server
	id            string
	l             serverLogger
}

func NewGotlin(options ...Option) (*Gotlin, error) {
	id := NewID().String()
	g := &Gotlin{
		SchedulerRepository:   NewSchedulerMemoryRepository(),
		ProgramRepository:     NewProgramMemoryRepository(),
		InstructionRepository: NewInstructionMemoryRepository(),
		ExecutorRepository:    NewExecutorMemoryRepository(),
		EnableServer:          false,
		ServerAddress:         ":9527",
		ServerExecutor:        true,
		GRPCOption:            []grpc.ServerOption{},
		id:                    id,
		l:                     serverLogger{}.WithServer(id),
	}

	for _, o := range options {
		o(g)
	}

	g.executorPool = NewExecutorPool(g.ExecutorRepository)
	g.schedulerPool = NewSchedulerPool(g.executorPool, g.SchedulerRepository, g.ProgramRepository, g.InstructionRepository)

	if g.ServerExecutor {
		err := g.executorPool.AddServerExecutor()
		if err != nil {
			return nil, err
		}
	}

	if g.EnableServer {
		server := grpc.NewServer(g.GRPCOption...)
		RegisterServerServiceServer(server, newServerService(g))
		g.gs = server
	}

	return g, nil
}

func (g *Gotlin) RequestScheduler(ctx context.Context, option SchedulerOption) (SchedulerID, error) {
	return g.schedulerPool.RequestScheduler(ctx, option)
}

func (g *Gotlin) RunProgramSync(ctx context.Context, s SchedulerID, p Program, ins []Instructioner) (interface{}, error) {
	return g.schedulerPool.RunProgramSync(ctx, s, p, ins)
}

func (g *Gotlin) RunProgram(ctx context.Context, s SchedulerID, p Program, ins []Instructioner) error {
	return g.schedulerPool.RunProgram(ctx, s, p, ins)
}

func (g *Gotlin) WaitResult(ctx context.Context) (chan ProgramResult, error) {
	return g.schedulerPool.WaitResult(ctx)
}

func (g *Gotlin) QueryResult(ctx context.Context, p Program) (interface{}, error) {
	return g.schedulerPool.QueryResult(ctx, p)
}

func (g *Gotlin) StartServer(ctx context.Context) (err error) {
	if g.gs == nil {
		return newError("gRPC server is not enabled")
	}

	lis, err := net.Listen("tcp", g.ServerAddress)
	if err != nil {
		return
	}

	return g.gs.Serve(lis)
}

func (g *Gotlin) StopServer(graceful bool) (err error) {
	if g.gs != nil {
		if graceful {
			g.gs.GracefulStop()
		} else {
			g.gs.Stop()
		}
	}
	return
}
