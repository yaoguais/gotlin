package gotlin

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"github.com/gofrs/uuid"
)

type ID struct {
	value uuid.UUID
	nonce uuid.UUID
}

func NewID() ID {
	v := ID{value: uuid.Must(uuid.NewV4())}
	if err := v.validate(); err != nil {
		panic(err)
	}
	return v
}

func ParseID(s string) (ID, error) {
	u, err := uuid.FromString(s)
	if err != nil {
		return ID{}, wrapError(err, "Parse id")
	}
	v := ID{value: u}
	return v, v.validate()
}

var emptyUUIDRegexp = regexp.MustCompile(`^[0-]+$`)

func (v ID) validate() error {
	isEmpty := emptyUUIDRegexp.MatchString(v.String())
	if isEmpty {
		return newError("Empty uuid found")
	}
	if v.value.Version() != uuid.V4 {
		return newError("Invalid uuid version")
	}
	return nil
}

func (v ID) IsValid() bool {
	return v.validate() == nil
}

func (v ID) String() string {
	return v.value.String()
}

func (v ID) NonceString() string {
	isEmpty := emptyUUIDRegexp.MatchString(v.nonce.String())
	if !isEmpty {
		return v.value.String() + ":" + v.nonce.String()
	}
	return v.value.String()

}

func (v ID) IsEqual(v2 ID) bool {
	return v.String() == v2.String()
}

func (v ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.NonceString())
}

func (v ID) changeNonce() ID {
	v.nonce = uuid.Must(uuid.NewV4())
	return v
}

func (v *ID) UnmarshalJSON(data []byte) error {
	if v == nil {
		return newError("ID is nil")
	}
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	ss := strings.Split(s, ":")
	n := len(ss)
	nonce := ID{}
	if n >= 2 {
		nonce, err = ParseID(ss[1])
		if err != nil {
			return err
		}
	}

	value, err := ParseID(ss[0])
	if err != nil {
		return err
	}
	*v = ID{value.value, nonce.value}
	return nil
}

type InstructionID = ID

func NewInstructionID() InstructionID {
	return InstructionID(NewID())
}

func ParseInstructionID(s string) (InstructionID, error) {
	v, err := ParseID(s)
	return InstructionID(v), err
}

type OpCode string

const (
	OpCodeWait      OpCode = "WAIT"
	OpCodeAdd       OpCode = "ADD"
	OpCodeSub       OpCode = "SUB"
	OpCodeMul       OpCode = "MUL"
	OpCodeDiv       OpCode = "DIV"
	OpCodeIn        OpCode = "IN"
	OpCodeMove      OpCode = "MOV"
	OpCodeIntersect OpCode = "INTERSECT"
	OpCodeUnion     OpCode = "UNION"
	OpCodeDiff      OpCode = "DIFF"
)

func ParseOpCode(s string) (OpCode, error) {
	if s == "" {
		return OpCode(""), newError("empty opcode found")
	}
	return OpCode(s), nil
}

func (v OpCode) String() string {
	return string(v)
}

type State string

const (
	StateNew     State = "New"
	StateReady   State = "Ready"
	StateRunning State = "Running"
	StateBlocked State = "Blocked"
	StateExit    State = "Exit"
)

type Timestamp int64

var nowFn = time.Now

const TimestampZero = Timestamp(0)

func NewTimestamp() Timestamp {
	time.Sleep(time.Millisecond) // TODO remove
	v := nowFn().UnixNano() / 1e6
	return Timestamp(v)
}

func (v Timestamp) Value() int64 {
	return int64(v)
}

func ParseTimestamp(v int64) Timestamp {
	return Timestamp(v)
}

func (v Timestamp) IsEqual(v2 Timestamp) bool {
	return v == v2
}

type ProgramID = ID

func NewProgramID() ProgramID {
	return ProgramID(NewID())
}

func ParseProgramID(s string) (ProgramID, error) {
	v, err := ParseID(s)
	return ProgramID(v), err
}

type ProgramCode struct {
	Instructions []InstructionID
}

func NewProgramCode() ProgramCode {
	return ProgramCode{}
}

func (v ProgramCode) AddInstruction(id InstructionID) ProgramCode {
	v.Instructions = append(v.Instructions, id)
	return v
}

func (v ProgramCode) IsEqual(v2 ProgramCode) bool {
	s1, _ := json.Marshal(v)
	s2, _ := json.Marshal(v2)
	return len(s1) > 0 && bytes.Equal(s1, s2)
}

type ControlUnitType string

const (
	ControlUnitTypePC  ControlUnitType = "ProgramCounter"
	ControlUnitTypeDAG ControlUnitType = "DAG"
)

type ProcessorContext struct {
	ControlUnit ControlUnitType
	Core        int
	Data        string
}

func NewProcessorContext() ProcessorContext {
	return ProcessorContext{
		ControlUnit: ControlUnitTypePC,
		Core:        1,
		Data:        "",
	}
}

func NewDAGProcessorContext(d InstructionDAG, core int) ProcessorContext {
	return ProcessorContext{
		ControlUnit: ControlUnitTypeDAG,
		Core:        core,
		Data:        d.MarshalString(),
	}
}

func (v ProcessorContext) IsPC() bool {
	return v.ControlUnit == ControlUnitTypePC
}

func (v ProcessorContext) CurrentPC() (InstructionID, bool) {
	if len(v.Data) == 0 {
		return InstructionID{}, false
	}
	id, err := ParseInstructionID(v.Data)
	return id, err == nil
}

func (v ProcessorContext) ChangePC(id InstructionID) ProcessorContext {
	v.Data = id.String()
	return v
}

func (v ProcessorContext) IsDAG() bool {
	return v.ControlUnit == ControlUnitTypeDAG
}

type SchedulerID = ID

func NewSchedulerID() SchedulerID {
	return SchedulerID(NewID())
}

func ParseSchedulerID(s string) (SchedulerID, error) {
	v, err := ParseID(s)
	return SchedulerID(v), err
}

type ScheduledPrograms struct {
	Programs []ProgramID
}

func NewScheduledPrograms() ScheduledPrograms {
	return ScheduledPrograms{}
}

func (v ScheduledPrograms) AddProgram(id ProgramID) ScheduledPrograms {
	found := false
	for _, old := range v.Programs {
		if old.IsEqual(id) {
			found = true
			break
		}
	}
	if !found {
		v.Programs = append(v.Programs, id)
	}
	return v
}

type Map map[string]interface{}

type ExecutorID = ID

func NewExecutorID() ExecutorID {
	return ExecutorID(NewID())
}

func ParseExecutorID(s string) (ExecutorID, error) {
	v, err := ParseID(s)
	return ExecutorID(v), err
}

type Labels []Label

func NewLabels(kv ...string) Labels {
	n := len(kv)
	if n%2 != 0 {
		panic("The number of keys and values should be the same")
	}
	ls := []Label{}
	for i := 0; i < n; i = i + 2 {
		ls = append(ls, NewLabel(kv[i], kv[i+1]))
	}
	return ls
}

func (v Labels) ExistOpCode(op OpCode) bool {
	value, ok := v.Find(OpCodeLabelKey)
	if !ok {
		return false
	}
	ss := strings.Split(value, ",")
	for _, s := range ss {
		if s != "" && OpCode(s) == op {
			return true
		}
	}
	return false
}

func (v Labels) Find(key string) (string, bool) {
	for _, v2 := range v {
		if v2.Key == key {
			return v2.Value, true
		}
	}
	return "", false
}

func (v Labels) Add(l Label) Labels {
	exist := false
	v2 := Labels{}
	for _, v3 := range v {
		if v3.Key == l.Key {
			v2 = append(v2, l)
			exist = true
		} else {
			v2 = append(v2, v3)
		}
	}
	if !exist {
		v2 = append(v2, l)
	}
	return v2
}

const OpCodeLabelKey = "opcode"

type Label struct {
	Key   string
	Value string
}

func NewLabel(k, v string) Label {
	return Label{k, v}
}

type Host string

const EmptyHost = Host("")

func (v Host) String() string {
	return string(v)
}

func (v Host) IsEqual(v2 Host) bool {
	return v == v2
}

type Resource struct {
	CPU       int64
	Memory    int64
	Disk      int64
	Bandwidth int64
}

func NewEmptyResource() Resource {
	return Resource{}
}

type ClientID = ID

func NewClientID() ClientID {
	return ClientID(NewID())
}

type ExecuteID = ID

func NewExecuteID() ExecuteID {
	return ExecuteID(NewID())
}

func ParseExecuteID(s string) (ExecuteID, error) {
	v, err := ParseID(s)
	return ExecuteID(v), err
}
