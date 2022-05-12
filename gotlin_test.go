package gotlin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"
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

func assertProgramExecuteResult(t require.TestingT, excepted interface{}, actual interface{}) {
	resultJSON, _ := json.Marshal(actual)
	exceptedJSON, _ := json.Marshal(excepted)
	require.Equal(t, string(exceptedJSON), string(resultJSON))
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_ProgramCounterProcessor(t *testing.T) {
	ctx := context.Background()

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_ProgramCounterProcessor_WithDBRepository(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
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

	ins := []Instructioner{i1, i2, i3}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 92, result)
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_DAGProcessor(t *testing.T) {
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	testGotlinDAGProcessor(t, g)
}

func testGotlinDAGProcessor(t *testing.T, g *Gotlin) {
	ctx := context.Background()

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	d := NewInstructionDAG()

	ids := []InstructionID{}
	for _, v := range ins {
		ids = append(ids, v.Instruction().ID)
	}
	err := d.Add(ids...)
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

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

func BenchmarkGotlin_DAGProcessor(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkGotlinDAGProcessor(b)
	}
}

// Perform an arithmetic calculation "( 1 + 2 ) * ( 5 - 1 )", the expected result is 12
func benchmarkGotlinDAGProcessor(t require.TestingT) {
	ctx := context.Background()

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(5)
	i5 := NewInstruction().ChangeImmediateValue(1)
	i6 := NewInstruction().ChangeToArithmetic(OpCodeSub)
	i7 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5, i6, i7}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	d := NewInstructionDAG()

	ids := []InstructionID{}
	for _, v := range ins {
		ids = append(ids, v.Instruction().ID)
	}
	err = d.Add(ids...)
	require.Nil(t, err)

	err = d.AttachChildren(i3.ID, i1.ID, i2.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i6.ID, i4.ID, i5.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i7.ID, i3.ID, i6.ID)
	require.Nil(t, err)

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 8))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
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

		ins := []Instructioner{i1, i2, i3}

		p := NewProgram()
		for _, in := range ins {
			p = p.AddInstruction(in.Instruction().ID)
		}

		p, ok := p.ChangeState(StateReady)
		require.True(t, ok)

		result, err := g.RunProgramSync(ctx, s, p, ins)
		require.Nil(t, err)

		assertProgramExecuteResult(t, q.Result, result)
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

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
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

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	time.Sleep(3000 * time.Millisecond)

	p, err = g.ProgramRepository.Find(ctx, p.ID)
	require.Nil(t, err)
	id, err := ParseInstructionID(p.Processor.Data)
	require.Nil(t, err)
	in, err := g.InstructionRepository.Find(ctx, id)
	require.Nil(t, err)
	result, err := in.InstructionResult(ctx)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

// Perform an arithmetic calculation "( 2 + 2 ) * 2", the expected result is 8
func TestGotlin_InstructionRef(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	i1 := NewInstruction().ChangeImmediateValue(2)
	i2 := NewInstructionRef(i1)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstructionRef(i1)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	d := NewInstructionDAG()

	ids := []InstructionID{}
	for _, v := range ins {
		ids = append(ids, v.Instruction().ID)
	}
	err = d.Add(ids...)
	require.Nil(t, err)

	err = d.AttachChildren(i3.ID, i1.ID, i2.Instruction().ID)
	require.Nil(t, err)
	err = d.AttachChildren(i5.ID, i3.ID, i4.Instruction().ID)
	require.Nil(t, err)

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 3))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 8, result)
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_WaitResult(t *testing.T) {
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	testGotlinWaitResult(t, g)
}

func testGotlinWaitResult(t *testing.T, g *Gotlin) {
	ctx := context.Background()

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)

	ins := []Instructioner{i1, i2, i3, i4, i5}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx)
	require.Nil(t, err)

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ch, err := g.WaitResult(ctx)
	require.Nil(t, err)

	result := ProgramResult{}
	select {
	case <-time.After(time.Second):
	case result = <-ch:
	}

	require.Nil(t, result.Error)
	assertProgramExecuteResult(t, 12, result.Result)
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_ZetaRunParallel(t *testing.T) {
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	wg := &sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("#%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(name, func(t *testing.T) {
				testGotlinDAGProcessor(t, g)
				testGotlinWaitResult(t, g)
			})
		}()
	}
	wg.Wait()
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func TestGotlin_ZetaRunParallel2(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("#%d", i)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testGotlinDAGProcessor(t, g)
			testGotlinWaitResult(t, g)
		})
	}
}
