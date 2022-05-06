package gotlin

import (
	"context"
	"net"
	"sync"

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

	executorPool *ExecutorPool
	schedulers   map[SchedulerID]bool
	programs     map[ProgramID]bool
	instructions map[InstructionID]bool
	gs           *grpc.Server
	mu           sync.RWMutex
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
		schedulers:            make(map[SchedulerID]bool),
		programs:              make(map[ProgramID]bool),
		instructions:          make(map[InstructionID]bool),
	}

	for _, o := range options {
		o(g)
	}

	g.executorPool = NewExecutorPool(g.ExecutorRepository)

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

func (g *Gotlin) LoadScheduler(ctx context.Context, s Scheduler) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	exist := g.schedulers[s.ID]
	if exist {
		return ErrSchedulerDuplicated
	}

	err := g.SchedulerRepository.Save(ctx, &s)
	if err != nil {
		return err
	}

	g.schedulers[s.ID] = true

	return nil
}

func (g *Gotlin) LoadProgram(ctx context.Context, p Program, ins []Instruction) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, in := range ins {
		exist := g.instructions[in.ID]
		if exist {
			return ErrInstructionDuplicated
		}
	}

	exist := g.programs[p.ID]
	if exist {
		return ErrSchedulerDuplicated
	}

	for _, in := range ins {
		err := g.loadInstruction(ctx, in)
		if err != nil {
			return err
		}
	}

	return g.loadProgram(ctx, p)
}

func (g *Gotlin) AssignScheduler(ctx context.Context, s Scheduler, p Program) error {

	p, err := g.initProgram(ctx, s, p)
	if err != nil {
		return err
	}

	return g.runProgram(ctx, s, p)
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

func (g *Gotlin) loadProgram(ctx context.Context, p Program) error {
	err := g.ProgramRepository.Save(ctx, &p)
	if err != nil {
		return err
	}

	g.programs[p.ID] = true

	return nil
}

func (g *Gotlin) loadInstruction(ctx context.Context, in Instruction) error {
	err := g.InstructionRepository.Save(ctx, &in)
	if err != nil {
		return err
	}

	g.instructions[in.ID] = true

	return nil
}

func (g *Gotlin) initProgram(ctx context.Context, s Scheduler, p Program) (Program, error) {
	if !p.IsState(StateReady) {
		return Program{}, errors.Wrap(ErrProgramState, "Not ready")
	}

	s = s.AddProgram(p.ID)
	err := g.SchedulerRepository.Save(ctx, &s)
	if err != nil {
		return Program{}, err
	}

	p, ok := p.ChangeState(StateRunning)
	if !ok {
		return Program{}, errors.Wrap(ErrProgramState, "Change to running")
	}
	err = g.ProgramRepository.Save(ctx, &p)
	if err != nil {
		return Program{}, err
	}
	return p, nil
}

func (g *Gotlin) runProgram(ctx context.Context, s Scheduler, p Program) error {
	processor, err := g.getProcessor(p)
	if err != nil {
		return err
	}

	err = processor.Process(ctx, p)
	return errors.Wrap(err, "Process program")
}

func (g *Gotlin) getProcessor(p Program) (Processor, error) {
	if p.IsPCProcessor() {
		return NewPCProcessor(g.ProgramRepository, g.InstructionRepository, g.executorPool), nil
	}
	if p.IsDAGProcessor() {
		return NewDAGProcessor(g.ProgramRepository, g.InstructionRepository, g.executorPool), nil
	}
	return nil, errors.New("Processor cannot be parsed")
}
