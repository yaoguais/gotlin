package gotlin

import (
	"context"
	"net"

	"github.com/pkg/errors"
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
}

func NewGotlin(options ...Option) (*Gotlin, error) {
	g := &Gotlin{
		SchedulerRepository:   nil,
		ProgramRepository:     nil,
		InstructionRepository: nil,
		ExecutorRepository:    nil,
		EnableServer:          true,
		ServerAddress:         ":9527",
		ServerExecutor:        false,
		GRPCOption:            []grpc.ServerOption{},
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

func (g *Gotlin) RequestScheduler(ctx context.Context) (Scheduler, error) {
	return g.schedulerPool.RequestScheduler(ctx)
}

func (g *Gotlin) LoadProgram(ctx context.Context, p Program, ins []Instruction) error {
	return g.schedulerPool.LoadProgram(ctx, p, ins)
}

func (g *Gotlin) AssignScheduler(ctx context.Context, s Scheduler, p Program) error {
	return g.schedulerPool.AssignScheduler(ctx, s, p)
}

func (g *Gotlin) StartServer(ctx context.Context) (err error) {
	if g.gs == nil {
		return errors.New("gRPC server is not enabled")
	}

	lis, err := net.Listen("tcp", g.ServerAddress)
	if err != nil {
		return
	}

	return g.gs.Serve(lis)
}

func (g *Gotlin) StopServer(graceful bool) (err error) {
	if graceful {
		g.gs.GracefulStop()
		return
	}
	g.gs.Stop()
	return
}
