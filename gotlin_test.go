package gotlin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/profile"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
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
		Logger: glogger.Default.LogMode(glogger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return db
}

// Perform an arithmetic calculation "( 1 + 2 ) * ( 5 - 1 )", the expected result is 12
func getTestProgram(t require.TestingT) (Program, []Instructioner) {
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
	err := d.Add(ids...)
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

	return p, ins
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func getTestProgram2(t require.TestingT) (Program, []Instructioner) {
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

	return p, ins
}

// Perform an arithmetic calculation "( 2b + 1b ) * 4 / 1b", the expected result is 12
func getTestProgram3(t require.TestingT) (Program, []Instructioner) {
	b := int(100 * time.Millisecond)
	i1 := NewInstruction().ChangeToWait(2 * b)
	i2 := NewInstruction().ChangeToWait(1 * b)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)
	i6 := NewInstruction().ChangeImmediateValue(1 * b)
	i7 := NewInstruction().ChangeToArithmetic(OpCodeDiv)

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
	err := d.Add(ids...)
	require.Nil(t, err)

	err = d.AttachChildren(i3.ID, i1.ID, i2.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i5.ID, i3.ID, i4.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i7.ID, i5.ID, i6.ID)
	require.Nil(t, err)

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 8))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	return p, ins
}

// Perform an arithmetic calculation "( 1 + 2 ) * 4", the expected result is 12
func getTestProgram4(t require.TestingT) (Program, []Instructioner, *InstructionSet) {
	const OpCodeAdd2 OpCode = "ADD2"

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd2)
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

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 8))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	calls := int64(0)

	is := NewInstructionSet()
	handler := InstructionHandler{
		OpCode: OpCodeAdd2,
		Executor: func(ctx context.Context, op Instruction,
			args ...Instruction) (InstructionResult, error) {
			i := atomic.AddInt64(&calls, 1)
			if i == 1 {
				err := errors.Errorf("OpCode %s will return an error directly when it is executed for the first time", OpCodeAdd2)
				return InstructionResult{}, err
			}

			result := float64(0)
			for _, in := range args {
				tmp, _ := in.InstructionResult(ctx)
				result += cast.ToFloat64(tmp)
			}
			return NewRegisterResult(result), nil
		},
	}
	is.Register(handler)

	return p, ins, is
}

func assertProgramExecuteResult(t require.TestingT, excepted interface{}, actual interface{}) {
	resultJSON, _ := json.Marshal(actual)
	exceptedJSON, _ := json.Marshal(excepted)
	require.Equal(t, string(exceptedJSON), string(resultJSON))
}

func TestGotlin_ProgramCounterProcessor(t *testing.T) {
	ctx := context.Background()

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	p, ins := getTestProgram2(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

func TestGotlin_ProgramCounterProcessor_WithDBRepository(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	p, ins := getTestProgram2(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

// Perform an arithmetic calculation "48 + 44", the expected result is 92
func TestGotlin_DatabaseInput(t *testing.T) {
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
	d1 := NewDatabaseInput(testDriver, testDSN, "select age from test_users where name = 'Rick'", converters)
	i1 := NewInstruction().ChangeDatabaseInput(d1)

	d2 := NewDatabaseInput(testDriver, testDSN, "select age from test_users where name = 'Michonne'", converters)
	i2 := NewInstruction().ChangeDatabaseInput(d2)

	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)

	ins := []Instructioner{i1, i2, i3}

	p := NewProgram()
	for _, in := range ins {
		p = p.AddInstruction(in.Instruction().ID)
	}

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 92, result)
}

func TestGotlin_DAGProcessor(t *testing.T) {
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	testGotlinDAGProcessor(t, g)
}

func testGotlinDAGProcessor(t *testing.T, g *Gotlin) {
	ctx := context.Background()

	p, ins := getTestProgram(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
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

func benchmarkGotlinDAGProcessor(t require.TestingT) {
	ctx := context.Background()

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	p, ins := getTestProgram(t)

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
		s, err := g.RequestScheduler(ctx, NewSchedulerOption())
		require.Nil(t, err)

		converters := []QueryConverter{QueryConverterFlat}
		d1 := NewDatabaseInput(testDriver, testDSN, q.Query1, converters)
		i1 := NewInstruction().ChangeDatabaseInput(d1)

		d2 := NewDatabaseInput(testDriver, testDSN, q.Query2, converters)
		i2 := NewInstruction().ChangeDatabaseInput(d2)

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

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	result, err := g.RunProgramSync(ctx, s, p, ins)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 8, result)
}

func TestGotlin_WaitResult(t *testing.T) {
	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	testGotlinWaitResult(t, g)
}

func testGotlinWaitResult(t *testing.T, g *Gotlin) {
	ctx := context.Background()

	p, ins := getTestProgram(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	ch, err := g.WaitResult(ctx, []ProgramID{p.ID})
	require.Nil(t, err)

	result := <-ch
	require.Nil(t, result.Error)
	assertProgramExecuteResult(t, 12, result.Result)
}

func TestGotlin_RerunProgram(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	p, ins, is := getTestProgram4(t)

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(false), WithInstructionSet(is))
	require.Nil(t, err)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	ch, err := g.WaitResult(context.Background(), []ProgramID{p.ID})
	require.Nil(t, err)
	result := <-ch
	require.NotNil(t, result.Error)
	require.Contains(t, result.Error.Error(), "ADD2")

	ctx = context.Background()
	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	ch, err = g.WaitResult(ctx, []ProgramID{p.ID})
	require.Nil(t, err)

	result = <-ch
	require.Nil(t, result.Error)
	assertProgramExecuteResult(t, 12, result.Result)
}

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

func TestGotlin_ZetaDAGProcessor_CPUProfile(t *testing.T) {
	pp := profile.Start(profile.CPUProfile, profile.ProfilePath("."))

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	for i := 0; i < 50; i++ {
		testGotlinDAGProcessor(t, g)
	}

	pp.Stop()

	output, err := exec.Command("go", "tool", "pprof", "-hide", "^runtime", "-top", "cpu.pprof").CombinedOutput()
	t.Logf("CPUProfile: error %v, output %s\n", err, string(output))
}

func TestGotlin_ZetaDAGProcessor_MemoryProfile(t *testing.T) {
	pp := profile.Start(profile.MemProfile, profile.MemProfileAllocs, profile.ProfilePath("."))

	g, err := NewGotlin(WithServerExecutor(true), WithEnableServer(false))
	require.Nil(t, err)

	for i := 0; i < 50; i++ {
		testGotlinDAGProcessor(t, g)
	}

	pp.Stop()

	output, err := exec.Command("go", "tool", "pprof", "-hide", "^runtime", "-top", "mem.pprof").CombinedOutput()
	t.Logf("MemoryProfile: error %v, output %s\n", err, string(output))
}
