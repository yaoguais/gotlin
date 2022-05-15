package gotlin

import (
	"net"
)

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

func newExecutorFromClient(r *RegisterExecutorRequest) (e Executor, err error) {
	id, err := ParseExecutorID(r.Id)
	if err != nil {
		return Executor{}, wrapError(err, "Parse Executor id")
	}
	_, _, err = net.SplitHostPort(r.Host)
	if err != nil {
		return Executor{}, wrapError(err, "Parse Host")
	}
	ls := []string{}
	for _, v := range r.Labels {
		ls = append(ls, v.Key)
		ls = append(ls, v.Value)
	}
	labels := NewLabels(ls...)
	_, ok := labels.Find(OpCodeLabelKey)
	if !ok {
		return Executor{}, newErrorf("Executor must have %s label", OpCodeLabelKey)
	}

	return Executor{
		ID:         id,
		Labels:     labels,
		Host:       Host(r.Host),
		State:      StateRunning,
		Error:      nil,
		Limits:     NewEmptyResource(),
		Usages:     NewEmptyResource(),
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}, nil
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

func (m Executor) ExitOnError(err error) Executor {
	m.Error = err
	m.State = StateExit
	m.FinishTime = NewTimestamp()
	return m
}
