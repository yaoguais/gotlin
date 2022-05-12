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
	wg           sync.WaitGroup
	pub          chan ProgramResult
	sub          map[int]chan ProgramResult
	i            int
	once         sync.Once
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
		pub:          make(chan ProgramResult, 1024),
		sub:          make(map[int]chan ProgramResult),
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

func (sp *SchedulerPool) RunProgramSync(ctx context.Context, s Scheduler, p Program, ins []Instructioner) (interface{}, error) {
	p, err := sp.saveProgram(ctx, p, ins)
	if err != nil {
		return nil, err
	}

	p, err = sp.initProgram(ctx, s, p)
	if err != nil {
		return nil, err
	}

	err = sp.runProgramSync(ctx, s, p)
	if err != nil {
		return nil, err
	}

	return sp.queryResult(ctx, p)
}

func (sp *SchedulerPool) RunProgram(ctx context.Context, s Scheduler, p Program, ins []Instructioner) error {
	p, err := sp.saveProgram(ctx, p, ins)
	if err != nil {
		return err
	}

	p, err = sp.initProgram(ctx, s, p)
	if err != nil {
		return err
	}

	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()

		err := sp.runProgramSync(ctx, s, p)
		if err != nil {
			i := ProgramResult{ID: p.ID, Error: err}
			sp.pub <- i
			return
		}

		result, err := sp.queryResult(ctx, p)
		i := ProgramResult{p.ID, result, err}
		sp.pub <- i
	}()

	return nil

}

func (sp *SchedulerPool) WaitResult(ctx context.Context) (chan ProgramResult, error) {

	sp.once.Do(func() {
		go func() {
			for v := range sp.pub {
				sp.mu.RLock()
				for _, ch := range sp.sub {
					ch := ch
					v := v
					go func() {
						ch <- v
					}()
				}
				sp.mu.RUnlock()
			}
		}()
	})

	ch := make(chan ProgramResult, 1024)
	sp.mu.Lock()
	i := sp.i
	sp.i++
	sp.sub[i] = ch
	sp.mu.Unlock()

	go func() {
		<-ctx.Done()
		close(ch)
		sp.mu.Lock()
		delete(sp.sub, i)
		sp.mu.Unlock()
	}()

	return ch, nil
}

func (sp *SchedulerPool) Close() error {
	sp.wg.Wait()

	sp.mu.Lock()
	if sp.pub != nil {
		close(sp.pub)
		sp.pub = nil
	}
	sp.mu.Unlock()

	return nil
}

func (sp *SchedulerPool) saveProgram(ctx context.Context, p Program, ins []Instructioner) (Program, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, iner := range ins {
		in := iner.Instruction()
		exist := sp.instructions[in.ID]
		_, isRef := iner.(InstructionRefer)
		if exist && !isRef {
			return Program{}, ErrInstructionDuplicated
		}
	}

	exist := sp.programs[p.ID]
	if exist {
		return Program{}, ErrProgramDuplicated
	}

	for _, iner := range ins {
		err := sp.saveInstruction(ctx, iner)
		if err != nil {
			return Program{}, err
		}
	}

	err := sp.ProgramRepository.Save(ctx, &p)
	if err != nil {
		return Program{}, err
	}

	for _, iner := range ins {
		sp.instructions[iner.Instruction().ID] = true
	}
	sp.programs[p.ID] = true

	return p, nil
}

func (sp *SchedulerPool) saveInstruction(ctx context.Context, iner Instructioner) (err error) {
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

	return nil
}

func (sp *SchedulerPool) saveScheduler(ctx context.Context, s Scheduler, p Program) error {
	s, err := sp.SchedulerRepository.Find(ctx, s.ID)
	if err != nil {
		return err
	}

	s = s.AddProgram(p.ID)
	return sp.SchedulerRepository.Save(ctx, &s)
}

func (sp *SchedulerPool) initProgram(ctx context.Context, s Scheduler, p Program) (Program, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if !p.IsState(StateReady) {
		return Program{}, errors.Wrap(ErrProgramState, "Not ready")
	}

	err := sp.saveScheduler(ctx, s, p)
	if err != nil {
		return Program{}, err
	}

	p, ok := p.ChangeState(StateRunning)
	if !ok {
		return Program{}, errors.Wrap(ErrProgramState, "Change to running")
	}
	err = sp.ProgramRepository.Save(ctx, &p)
	return p, err
}

func (sp *SchedulerPool) runProgramSync(ctx context.Context, s Scheduler, p Program) error {
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

func (sp *SchedulerPool) queryResult(ctx context.Context, p Program) (interface{}, error) {
	p, err := sp.ProgramRepository.Find(ctx, p.ID)
	if err != nil {
		return nil, err
	}

	if !p.IsState(StateExit) {
		return nil, errors.Wrap(ErrProgramState, "Is not exit")
	}

	if p.Error != nil {
		return nil, p.Error
	}

	if p.IsPCProcessor() {
		id, err := ParseInstructionID(p.Processor.Data)
		if err != nil {
			return nil, err
		}
		in, err := sp.InstructionRepository.Find(ctx, id)
		if err != nil {
			return nil, err
		}
		return in.InstructionResult(ctx)
	}

	if p.IsDAGProcessor() {
		d, err := ParseInstructionDAG(p.Processor.Data)
		if err != nil {
			return nil, err
		}
		ans := d.Ancestors()
		if len(ans) == 0 {
			return nil, errors.New("No instruction found")
		}
		id := ans[0]

		in, err := sp.InstructionRepository.Find(ctx, id)
		if err != nil {
			return nil, err
		}
		return in.InstructionResult(ctx)
	}

	return nil, errors.New("The type of Processor is wrong")
}
