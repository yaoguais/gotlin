package gotlin

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrSchedulerDuplicated   = errors.New("Scheduler duplicated")
	ErrProgramDuplicated     = errors.New("Program duplicated")
	ErrInstructionDuplicated = errors.New("Instruction duplicated")
)

type Gotlin struct {
	SchedulerRepository   SchedulerRepository
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
	ExecutorRepository    ExecutorRepository
	ServerExecutor        bool

	executorPool *ExecutorPool
	schedulers   map[SchedulerID]bool
	programs     map[ProgramID]bool
	instructions map[InstructionID]bool
	mu           sync.RWMutex
}

func NewGotlin(options ...Option) (*Gotlin, error) {
	g := &Gotlin{
		schedulers:   make(map[SchedulerID]bool),
		programs:     make(map[ProgramID]bool),
		instructions: make(map[InstructionID]bool),
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
