package gotlin

type Program struct {
	ID         ProgramID
	Code       ProgramCode
	State      State
	Error      error
	Processor  ProcessorContext
	CreateTime Timestamp
	UpdateTime Timestamp
	FinishTime Timestamp
}

func NewProgram() Program {
	return Program{
		ID:         NewProgramID(),
		Code:       NewProgramCode(),
		State:      StateNew,
		Error:      nil,
		Processor:  NewProcessorContext(),
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}
}

func (m Program) AddInstruction(id InstructionID) Program {
	m.Code = m.Code.AddInstruction(id)
	return m
}

func (m Program) IsPCProcessor() bool {
	return m.Processor.IsPC()
}

func (m Program) NextPC(id InstructionID) Program {
	m.Processor = m.Processor.ChangePC(id)
	return m
}

func (m Program) ChangeProcessor(p ProcessorContext) Program {
	m.Processor = p
	return m
}

func (m Program) IsDAGProcessor() bool {
	return m.Processor.IsDAG()
}

func (m Program) IsState(state State) bool {
	return m.State == state
}

func (m Program) ChangeState(state State) (Program, bool) {
	m.State = state
	return m, true
}

func (m Program) ExitOnError(err error) Program {
	m.Error = err
	m.State = StateExit
	m.FinishTime = NewTimestamp()
	return m
}
