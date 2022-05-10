package gotlin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var testDriver = "mysql"
var testDSN string

func init() {
	dsn := "root:%s@tcp(127.0.0.1:3306)/gotlin?charset=utf8mb4&parseTime=True&loc=Local"
	password := os.Getenv("MYSQL_ROOT_PASSWORD")
	testDSN = fmt.Sprintf(dsn, password)

	db := getTestDB()
	tableSQLs, _ := os.ReadFile("./schema.sql")
	for _, s := range strings.Split(string(tableSQLs), ";\n") {
		s = strings.TrimSpace(s)
		if s != "" {
			_ = db.Exec(s).Error
		}
	}

	go func() {
		http.ListenAndServe(":8000", nil)
	}()
}

func getTestDB() *gorm.DB {
	db, err := gorm.Open(mysql.Open(testDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return db
}

func assertProgramExecuteResult(t *testing.T, excepted interface{}, g *Gotlin, p Program) {
	ctx := context.Background()

	p, err := g.ProgramRepository.Find(ctx, p.ID)
	require.Nil(t, err)
	require.Nil(t, p.Error)

	var instructionID InstructionID
	if p.IsPCProcessor() {
		instructionID, err = ParseInstructionID(p.Processor.Data)
		require.Nil(t, err)
	} else if p.IsDAGProcessor() {
		d, err := ParseInstructionDAG(p.Processor.Data)
		require.Nil(t, err)
		ans := d.Ancestors()
		require.Equal(t, 1, len(ans))
		instructionID = ans[0]
	} else {
		require.Equal(t, "The type of Processor is wrong", "")
	}

	in, err := g.InstructionRepository.Find(ctx, instructionID)
	require.Nil(t, err)
	result, err := in.InstructionResult(ctx)
	require.Nil(t, err)
	resultJSON, _ := json.Marshal(result)
	exceptedJSON, _ := json.Marshal(excepted)
	require.Equal(t, string(exceptedJSON), string(resultJSON))
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_ProgramCounterProcessor(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instruction{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, g, p)
}

// Perform an arithmetic calculation "48 + 44", the expected result is 92
func TestGotlin_DatabaseQuery(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	var err error
	defer func() {
		if r := recover(); err != nil {
			fmt.Printf("recover: %v\n", r)
		}
		cleanSQL := "DROP TABLE test_users"
		_ = db.Exec(cleanSQL).Error
		require.Nil(t, err)
	}()

	preSQLs := []string{
		"CREATE TABLE IF NOT EXISTS test_users(id int(10) PRIMARY KEY, name varchar(50) NOT NULL, age int(10) NOT NULL)",
		"INSERT INTO test_users VALUES(1, 'Rick', 48), (2, 'Michonne', 44)",
	}
	for _, preSQL := range preSQLs {
		err = db.Exec(preSQL).Error
		require.Nil(t, err)
	}

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	converters := []QueryConverter{QueryConverterFirstValue}
	d1 := NewDatabaseQuery(testDriver, testDSN, "select age from test_users where name = 'Rick'", converters)
	i1 := NewInstruction().ChangeDatabaseQuery(d1)

	d2 := NewDatabaseQuery(testDriver, testDSN, "select age from test_users where name = 'Michonne'", converters)
	i2 := NewInstruction().ChangeDatabaseQuery(d2)

	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)

	ins := []Instruction{i1, i2, i3}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 92, g, p)
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_DAGProcessor(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instruction{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.ID)
	}

	d := NewInstructionDAG()

	ids := []InstructionID{}
	for _, v := range ins {
		ids = append(ids, v.ID)
	}
	err = d.Add(ids...)
	require.Nil(t, err)

	err = d.AttachChildren(i3.ID, i1.ID, i2.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i5.ID, i3.ID, i4.ID)
	require.Nil(t, err)

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 3))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)

	ans := d.Ancestors()
	require.Equal(t, 1, len(ans))

	assertProgramExecuteResult(t, 12, g, p)
}

func TestGotlin_CollectionInstruction(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	var err error
	defer func() {
		if r := recover(); err != nil {
			fmt.Printf("recover: %v\n", r)
		}
		cleanSQL := "DROP TABLE test_collections"
		_ = db.Exec(cleanSQL).Error
		require.Nil(t, err)
	}()

	preSQLs := []string{
		"CREATE TABLE IF NOT EXISTS test_collections(id int(10) PRIMARY KEY, name varchar(50) NOT NULL, score decimal(8,2) NOT NULL)",
		"INSERT INTO test_collections VALUES(1, 'C1', 0.2), (2, 'C2', 0.2)",
		"INSERT INTO test_collections VALUES(3, 'C3', 1.2), (4, 'C3', 2.4)",
	}
	for _, preSQL := range preSQLs {
		err = db.Exec(preSQL).Error
		require.Nil(t, err)
	}

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	qs := []struct {
		Query1 string
		Query2 string
		OpCode OpCode
		Result interface{}
	}{
		{
			"select name from test_collections where id IN (1, 3)",
			"select name from test_collections where id IN (2, 4)",
			OpCodeIntersect,
			[]string{"C3"},
		},
		{
			"select id from test_collections where id IN (1, 3)",
			"select id from test_collections where id IN (3, 4)",
			OpCodeUnion,
			[]int{1, 3, 4},
		},
		{
			"select score from test_collections where id IN (1, 3, 4)",
			"select score from test_collections where id IN (2, 3)",
			OpCodeDiff,
			[]float64{2.4},
		},
	}

	for _, q := range qs {
		s, err := g.RequestScheduler(ctx)
		require.Nil(t, err)

		converters := []QueryConverter{QueryConverterFlat}
		d1 := NewDatabaseQuery(testDriver, testDSN, q.Query1, converters)
		i1 := NewInstruction().ChangeDatabaseQuery(d1)

		d2 := NewDatabaseQuery(testDriver, testDSN, q.Query2, converters)
		i2 := NewInstruction().ChangeDatabaseQuery(d2)

		i3 := NewInstruction().ChangeToArithmetic(q.OpCode)

		ins := []Instruction{i1, i2, i3}

		p := NewProgram()
		for _, in := range ins {
			p = p.AddInstruction(in.ID)
		}

		p, ok := p.ChangeState(StateReady)
		require.True(t, ok)

		err = g.LoadProgram(ctx, p, ins)
		require.Nil(t, err)

		err = g.AssignScheduler(ctx, s, p)
		require.Nil(t, err)

		assertProgramExecuteResult(t, q.Result, g, p)
	}
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_RemoteExecutorViaGRPC(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(false), WithEnableServer(true))
	require.Nil(t, err)

	go func() {
		err := g.StartServer(ctx)
		if err != nil {
			panic(err)
		}
	}()
	defer g.StopServer(false)

	time.Sleep(100 * time.Millisecond)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instruction{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	c, err := NewClient(WithClientGRPCOptions(grpc.WithInsecure()))
	require.Nil(t, err)

	params := RegisterExecutorOption{
		ID:     NewExecutorID(),
		Host:   "127.0.0.1:0",
		Labels: (&ExecutorPool{}).getDefaultExecutor().Labels, // TODO fix it
	}
	err = c.RegisterExecutor(ctx, params)
	require.Nil(t, err)

	go func() {
		err := c.StartComputeNode(ctx, StartComputeNodeOption{})
		fmt.Printf("Loop commands, %v\n", err)
	}()

	time.Sleep(100 * time.Millisecond)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)

	time.Sleep(3000 * time.Millisecond)

	assertProgramExecuteResult(t, 12, g, p)
}
