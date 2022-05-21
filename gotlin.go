package gotlin

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"net"
	"os"

	"github.com/vmihailenco/msgpack/v5"
	. "github.com/yaoguais/gotlin/proto" //revive:disable-line
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

type Option func(g *Gotlin)

func WithDatabase(db *gorm.DB) Option {
	return func(g *Gotlin) {
		g.SchedulerRepository = newSchedulerDBRepository(db)
		g.ProgramRepository = newProgramDBRepository(db)
		g.InstructionRepository = newInstructionDBRepository(db)
		g.ExecutorRepository = newExecutorDBRepository(db)
	}
}

func WithServerExecutor(enable bool) Option {
	return func(g *Gotlin) {
		g.ServerExecutor = enable
	}
}

func WithEnableServer(enable bool) Option {
	return func(g *Gotlin) {
		g.EnableServer = enable
	}
}

func WithServerAddress(addr string) Option {
	return func(g *Gotlin) {
		g.ServerAddress = addr
	}
}

func WithInstructionSet(is *InstructionSet) Option {
	return func(g *Gotlin) {
		g.is = is
	}
}

func WithGRPCServerOption(options ...grpc.ServerOption) Option {
	return func(g *Gotlin) {
		g.GRPCOption = options
	}
}

type (
	MarshalType string
	Marshal     func(v interface{}) ([]byte, error)
	Unmarshal   func(data []byte, v interface{}) error
)

var (
	MarshalTypeJSON    MarshalType = "json"
	MarshalTypeMsgPack MarshalType = "msgpack"
	MarshalTypeGob     MarshalType = "gob"
	marshal            Marshal     = json.Marshal
	unmarshal          Unmarshal   = json.Unmarshal
)

var (
	MarshalStructs = []interface{}{
		Operand{},
		EmptyInput{},
		Immediate{},
		DatabaseInput{},
		InstructionResult{},
		EmptyResult{},
		RegisterResult{},
		[]interface{}{},
	}
)

var EnvPrefix = "GOTLIN_"

func getenv(s string) string {
	return os.Getenv(EnvPrefix + s)
}

func init() {
	for _, v := range MarshalStructs {
		gob.Register(v)
	}
	if s := getenv("MARSHAL_TYPE"); s != "" {
		SetMarshalType(MarshalType(s))
	}
}

func SetMarshalType(v MarshalType) {
	switch v {
	case MarshalTypeJSON:
		marshal = json.Marshal
		unmarshal = json.Unmarshal
	case MarshalTypeMsgPack:
		marshal = msgpack.Marshal
		unmarshal = msgpack.Unmarshal
	case MarshalTypeGob:
		marshal = gobMarshal
		unmarshal = gobUnmarshal
	default:
		panic("Undefined marshal type " + string(v))
	}
}

func gobMarshal(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(v)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func gobUnmarshal(data []byte, v interface{}) error {
	b := bytes.NewBuffer(data)
	return gob.NewDecoder(b).Decode(v)
}

type Gotlin struct {
	SchedulerRepository   SchedulerRepository
	ProgramRepository     ProgramRepository
	InstructionRepository InstructionRepository
	ExecutorRepository    ExecutorRepository
	EnableServer          bool
	ServerAddress         string
	ServerExecutor        bool
	GRPCOption            []grpc.ServerOption

	executorPool  *ExecutorPool
	schedulerPool *SchedulerPool
	is            *InstructionSet
	gs            *grpc.Server
	id            string
	l             serverLogger
}

func NewGotlin(options ...Option) (*Gotlin, error) {
	id := NewID().String()
	g := &Gotlin{
		SchedulerRepository:   newSchedulerMemoryRepository(),
		ProgramRepository:     newProgramMemoryRepository(),
		InstructionRepository: newInstructionMemoryRepository(),
		ExecutorRepository:    newExecutorMemoryRepository(),
		EnableServer:          false,
		ServerAddress:         ":9527",
		ServerExecutor:        true,
		GRPCOption:            []grpc.ServerOption{},
		is:                    NewInstructionSet(),
		id:                    id,
		l:                     serverLogger{}.WithServer(id),
	}

	for _, o := range options {
		o(g)
	}

	g.executorPool = NewExecutorPool(g.ExecutorRepository, g.is)
	g.schedulerPool = NewSchedulerPool(g.executorPool, g.SchedulerRepository, g.ProgramRepository, g.InstructionRepository)

	if g.ServerExecutor {
		err := g.executorPool.AddServerExecutor()
		if err != nil {
			return nil, err
		}
	}

	if g.EnableServer {
		server := grpc.NewServer(g.GRPCOption...)
		RegisterServerServiceServer(server, newServerService(g))
		g.gs = server
	}

	return g, nil
}

func (g *Gotlin) RequestScheduler(ctx context.Context, option SchedulerOption) (SchedulerID, error) {
	return g.schedulerPool.RequestScheduler(ctx, option)
}

func (g *Gotlin) RunProgramSync(ctx context.Context, s SchedulerID, p Program, ins []Instructioner) (interface{}, error) {
	return g.schedulerPool.RunProgramSync(ctx, s, p, ins)
}

func (g *Gotlin) RunProgram(ctx context.Context, s SchedulerID, p Program, ins []Instructioner) error {
	return g.schedulerPool.RunProgram(ctx, s, p, ins)
}

func (g *Gotlin) WaitResult(ctx context.Context, ids []ProgramID) (chan ProgramResult, error) {
	return g.schedulerPool.WaitResult(ctx, ids)
}

func (g *Gotlin) QueryResult(ctx context.Context, p Program) (interface{}, error) {
	return g.schedulerPool.QueryResult(ctx, p)
}

func (g *Gotlin) StartServer(ctx context.Context) (err error) {
	if g.gs == nil {
		return newError("gRPC server is not enabled")
	}

	lis, err := net.Listen("tcp", g.ServerAddress)
	if err != nil {
		return
	}

	metrics.Up()

	return g.gs.Serve(lis)
}

func (g *Gotlin) StopServer(graceful bool) (err error) {
	if g.gs != nil {
		if graceful {
			g.gs.GracefulStop()
		} else {
			g.gs.Stop()
		}
	}
	return
}
