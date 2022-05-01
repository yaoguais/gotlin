package gotlin

type Scheduler struct {
	ID         SchedulerID
	Programs   ScheduledPrograms
	CreateTime Timestamp
	UpdateTime Timestamp
	FinishTime Timestamp
}

func NewScheduler() Scheduler {
	return Scheduler{
		ID:         NewSchedulerID(),
		Programs:   NewScheduledPrograms(),
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}
}

func (m Scheduler) AddProgram(id ProgramID) Scheduler {
	m.Programs = m.Programs.AddProgram(id)
	return m
}
