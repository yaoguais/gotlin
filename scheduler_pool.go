package gotlin

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

type SchedulerPool struct {
	ExecutorPool          *ExecutorPool
	SchedulerRepository   SchedulerRepository
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository

	schedulers   map[SchedulerID]bool
	programs     map[ProgramID]bool
	instructions map[InstructionID]bool
	mu           sync.RWMutex
}

func NewSchedulerPool(ep *ExecutorPool, sr SchedulerRepository, pr ProgramRepository, ir InstructionRepository) *SchedulerPool {
	return &SchedulerPool{
		ExecutorPool:          ep,
		SchedulerRepository:   sr,
		ProgramRepository:     pr,
		InstructionRepository: ir,

		schedulers:   make(map[SchedulerID]bool),
		programs:     make(map[ProgramID]bool),
		instructions: make(map[InstructionID]bool),
	}
}

func (sp *SchedulerPool) RequestScheduler(ctx context.Context) (Scheduler, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if len(sp.schedulers) == 0 {
		s, err := sp.newScheduler(ctx)
		if err != nil {
			return Scheduler{}, err
		}
		sp.schedulers[s.ID] = true
		return s, nil
	}

	id := SchedulerID{}
	for id = range sp.schedulers {
	}

	s, err := sp.SchedulerRepository.Find(ctx, id)
	return s, errors.Wrap(err, "Find Scheduler")
}

func (sp *SchedulerPool) newScheduler(ctx context.Context) (Scheduler, error) {
	s := NewScheduler()
	exist := sp.schedulers[s.ID]
	if exist {
		return Scheduler{}, ErrSchedulerDuplicated
	}

	err := sp.SchedulerRepository.Save(ctx, &s)
	return s, errors.Wrap(err, "Save Scheduler to Repository")
}

func (sp *SchedulerPool) RunProgram(ctx context.Context, s Scheduler, p Program, ins []Instructioner) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	err := sp.loadProgram(ctx, p, ins)
	if err != nil {
		return err
	}

	p, err = sp.initProgram(ctx, s, p)
	if err != nil {
		return err
	}

	return sp.runProgram(ctx, s, p)
}

func (sp *SchedulerPool) loadProgram(ctx context.Context, p Program, ins []Instructioner) error {
	for _, iner := range ins {
		in := iner.Instruction()
		exist := sp.instructions[in.ID]
		_, isRef := iner.(InstructionRefer)
		if exist && !isRef {
			return ErrInstructionDuplicated
		}
	}

	exist := sp.programs[p.ID]
	if exist {
		return ErrSchedulerDuplicated
	}

	for _, iner := range ins {
		err := sp.loadInstruction(ctx, iner)
		if err != nil {
			return err
		}
	}

	err := sp.ProgramRepository.Save(ctx, &p)
	if err != nil {
		return err
	}

	sp.programs[p.ID] = true

	return nil
}

func (sp *SchedulerPool) loadInstruction(ctx context.Context, iner Instructioner) (err error) {
	in := iner.Instruction()
	_, isRef := iner.(InstructionRefer)

	isSave := true

	if isRef {
		_, err = sp.InstructionRepository.Find(ctx, in.ID)
		notFound := isRecordNotFound(err)
		if err != nil && !notFound {
			return
		} else if !notFound {
			isSave = false
		}
	}

	if isSave {
		err = sp.InstructionRepository.Save(ctx, &in)
		if err != nil {
			return
		}
	}

	sp.instructions[in.ID] = true

	return nil
}

func (sp *SchedulerPool) initProgram(ctx context.Context, s Scheduler, p Program) (Program, error) {
	if !p.IsState(StateReady) {
		return Program{}, errors.Wrap(ErrProgramState, "Not ready")
	}

	s = s.AddProgram(p.ID)
	err := sp.SchedulerRepository.Save(ctx, &s)
	if err != nil {
		return Program{}, err
	}

	p, ok := p.ChangeState(StateRunning)
	if !ok {
		return Program{}, errors.Wrap(ErrProgramState, "Change to running")
	}
	err = sp.ProgramRepository.Save(ctx, &p)
	if err != nil {
		return Program{}, err
	}
	return p, nil
}

func (sp *SchedulerPool) runProgram(ctx context.Context, s Scheduler, p Program) error {
	processor, err := sp.getProcessor(p)
	if err != nil {
		return err
	}

	err = processor.Process(ctx, p)
	return errors.Wrap(err, "Process program")
}

func (sp *SchedulerPool) getProcessor(p Program) (Processor, error) {
	if p.IsPCProcessor() {
		return NewPCProcessor(sp.ProgramRepository, sp.InstructionRepository, sp.ExecutorPool), nil
	}
	if p.IsDAGProcessor() {
		return NewDAGProcessor(sp.ProgramRepository, sp.InstructionRepository, sp.ExecutorPool), nil
	}
	return nil, errors.New("Processor cannot be parsed")
}
