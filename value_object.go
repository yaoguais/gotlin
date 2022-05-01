package gotlin

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

var (
	BinaryMarshalFunc   = json.Marshal
	BinaryUnmarshalFunc = json.Unmarshal
)

type ID struct {
	value uuid.UUID
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
		return ID{}, errors.Wrap(err, "Parse id")
	}
	v := ID{value: u}
	return v, v.validate()
}

var emptyUUIDRegexp = regexp.MustCompile(`^[0-]+$`)

func (v ID) validate() error {
	isEmpty := emptyUUIDRegexp.MatchString(v.String())
	if isEmpty {
		return errors.New("Empty uuid found")
	}
	if v.value.Version() != uuid.V4 {
		return errors.New("Invalid uuid version")
	}
	return nil
}

func (v ID) String() string {
	return v.value.String()
}

func (v ID) IsEqual(v2 ID) bool {
	return v.String() == v2.String()
}

func (v ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v *ID) UnmarshalJSON(data []byte) error {
	if v == nil {
		return errors.New("ID is nil")
	}
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	v2, err := ParseID(s)
	if err != nil {
		return err
	}
	*v = v2
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
	OpCodeAdd  OpCode = "ADD"
	OpCodeSub  OpCode = "SUB"
	OpCodeMul  OpCode = "MUL"
	OpCodeDiv  OpCode = "DIV"
	OpCodeIn   OpCode = "IN"
	OpCodeMove OpCode = "MOV"
)

func ParseOpCode(s string) (OpCode, error) {
	if s == "" {
		return OpCode(""), errors.New("empty opcode found")
	}
	return OpCode(s), nil
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

type ControlUnitType string

const (
	ControlUnitTypePC  ControlUnitType = "program_counter"
	ControlUnitTypeDAG ControlUnitType = "dag"
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

func (v ProcessorContext) IsPC() bool {
	return v.ControlUnit == ControlUnitTypePC
}

func (v ProcessorContext) IsSync() bool {
	return v.Core <= 1 || v.ControlUnit == ControlUnitTypePC
}

func (v ProcessorContext) IsConcurrently() bool {
	return v.Core > 1 && v.ControlUnit == ControlUnitTypeDAG
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
		if old == id {
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