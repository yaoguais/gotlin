package gotlin

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var testDriver = "mysql"
var testDSN string

func init() {
	dsn := "root:%s@tcp(127.0.0.1:3306)/gotlin?charset=utf8mb4&parseTime=True&loc=Local"
	password := os.Getenv("MYSQL_ROOT_PASSWORE")
	testDSN = fmt.Sprintf(dsn, password)

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

func TestGotlin_ProgramCounterProcessor(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g := NewGotlin(WithDatabase(db))

	s := NewScheduler()

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

	err := g.LoadScheduler(ctx, s)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)
}

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

	g := NewGotlin(WithDatabase(db))

	s := NewScheduler()

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

	err = g.LoadScheduler(ctx, s)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)
}

func TestGotlin_DAGProcessor(t *testing.T) {
	ctx := context.Background()

	db := getTestDB()

	g := NewGotlin(WithDatabase(db))

	s := NewScheduler()

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
	err := d.Add(ids...)
	require.Nil(t, err)

	err = d.AttachChildren(i3.ID, i1.ID, i2.ID)
	require.Nil(t, err)
	err = d.AttachChildren(i5.ID, i3.ID, i4.ID)
	require.Nil(t, err)

	p = p.ChangeProcessor(NewDAGProcessorContext(d, 3))

	p, ok := p.ChangeState(StateReady)
	require.True(t, ok)

	err = g.LoadScheduler(ctx, s)
	require.Nil(t, err)

	err = g.LoadProgram(ctx, p, ins)
	require.Nil(t, err)

	err = g.AssignScheduler(ctx, s, p)
	require.Nil(t, err)

	ans := d.Ancestors()
	require.Equal(t, 1, len(ans))

	in, err := g.InstructionRepository.Find(ctx, ans[0])
	require.Nil(t, err)
	require.Nil(t, in.Error)
	value, err := in.InstructionResult(ctx)
	require.Nil(t, err)
	require.Equal(t, 12, cast.ToInt(value))
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

	g := NewGotlin(WithDatabase(db))

	s := NewScheduler()

	err = g.LoadScheduler(ctx, s)
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
			nil,
		},
	}

	for _, q := range qs {
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
	}
}
