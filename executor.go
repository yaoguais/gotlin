package gotlin

type Executor struct {
	ID         ExecutorID
	Labels     Labels
	Host       Host
	State      State
	Error      error
	Limits     Resource
	Usages     Resource
	CreateTime Timestamp
	UpdateTime Timestamp
	FinishTime Timestamp
}

func NewExecutor() Executor {
	return Executor{
		ID:         NewExecutorID(),
		Labels:     NewLabels(),
		Host:       EmptyHost,
		State:      StateNew,
		Error:      nil,
		Limits:     NewEmptyResource(),
		Usages:     NewEmptyResource(),
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}
}

func (m Executor) AddLabel(label Label) Executor {
	m.Labels = m.Labels.Add(label)
	return m
}

func (m Executor) IsState(state State) bool {
	return m.State == state
}

func (m Executor) ChangeState(state State) (Executor, bool) {
	m.State = state
	return m, true
}

func (m Executor) IsEmptyHost() bool {
	return m.Host == EmptyHost
}
