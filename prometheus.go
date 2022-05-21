package gotlin

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metrics = prom{}

var (
	promNamespace   = "gotlin"
	promServer      = "server"
	promExecutor    = "executor"
	promProgram     = "program"
	promInstruction = "instruction"

	promHostname, _ = os.Hostname()
	promConstLabels = prometheus.Labels{"host": promHostname}

	serverUpTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   promNamespace,
		Subsystem:   promServer,
		Name:        "up_timestamp",
		Help:        "The millisecond timestamp when the service node was started",
		ConstLabels: promConstLabels,
	})

	numberOfRunningExecutor = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   promNamespace,
		Subsystem:   promExecutor,
		Name:        "running",
		Help:        "The number of currently running Executors",
		ConstLabels: promConstLabels,
	})

	totalExecutionTimeOfExecutor = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace:   promNamespace,
		Subsystem:   promExecutor,
		Name:        "execution_time_total",
		Help:        "Executor execution time statistics with or without error conditions",
		ConstLabels: promConstLabels,
	}, []string{"id", "error"})

	numberOfSubmittedProgram = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   promNamespace,
		Subsystem:   promProgram,
		Name:        "submitted_total",
		Help:        "The number of submitted Programs",
		ConstLabels: promConstLabels,
	})

	numberOfRunningProgram = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   promNamespace,
		Subsystem:   promProgram,
		Name:        "running",
		Help:        "The number of currently running Programs",
		ConstLabels: promConstLabels,
	})

	numberOfExecutedErrorProgram = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   promNamespace,
		Subsystem:   promProgram,
		Name:        "executed_error_total",
		Help:        "The number of Programs that executed error",
		ConstLabels: promConstLabels,
	})

	totalExecutionTimeOfOpCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace:   promNamespace,
		Subsystem:   promInstruction,
		Name:        "opcode_execution_time_total",
		Help:        "Instruction execution time statistics with or without error conditions",
		ConstLabels: promConstLabels,
	}, []string{"opcode", "error"})

	totalExecutionNumberOfOpCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace:   promNamespace,
		Subsystem:   promInstruction,
		Name:        "opcode_execution_number_total",
		Help:        "Instruction execution number statistics with or without error conditions",
		ConstLabels: promConstLabels,
	}, []string{"opcode", "error"})
)

type prom struct {
}

func (m prom) Up() {
	v := time.Now().UnixNano() / 1e6
	serverUpTimestamp.Set(float64(v))
}

func (m prom) AddExecutor() {
	numberOfRunningExecutor.Inc()
}

func (m prom) RemoveExecutor() {
	numberOfRunningExecutor.Dec()
}

func (m prom) Execute(id ExecutorID, d time.Duration, err error) {
	errValue := "0"
	if err != nil {
		errValue = "1"
	}

	v := d / 1e6
	totalExecutionTimeOfExecutor.WithLabelValues(id.String(), errValue).Add(float64(v))
}

func (m prom) AddRunningProgram() {
	numberOfSubmittedProgram.Inc()
	numberOfRunningProgram.Inc()
}

func (m prom) RemoveRunningProgram(err error) {
	numberOfRunningProgram.Dec()
	if err != nil {
		numberOfExecutedErrorProgram.Inc()
	}
}

func (m prom) ExecuteInstruction(in Instruction, d time.Duration, err error) {
	errValue := "0"
	if err != nil {
		errValue = "1"
	}

	v := d / 1e6
	totalExecutionTimeOfOpCode.WithLabelValues(in.OpCode.String(), errValue).Add(float64(v))
	totalExecutionNumberOfOpCode.WithLabelValues(in.OpCode.String(), errValue).Inc()
}
