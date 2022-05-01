package gotlin

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var testDriver = "mysql"
var testDSN string

func init() {
	dsn := "root:%s@tcp(127.0.0.1:3306)/gotlin?charset=utf8mb4&parseTime=True&loc=Local"
	password := os.Getenv("MYSQL_ROOT_PASSWORE")
	testDSN = fmt.Sprintf(dsn, password)
}

func getTestDB() *gorm.DB {
	db, err := gorm.Open(mysql.Open(testDSN), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return db
}

func TestGotlin_ProgramCounterProcessor(t *testing.T) {
	ctx := context.Background()

	db := getTestDB().Debug()

	g := NewGotlin(WithDatabase(db))

	s := NewScheduler()

	i1 := NewInstruction().ChangeImmediateValue(1)
	i2 := NewInstruction().ChangeImmediateValue(2)
	i3 := NewInstruction().ChangeToArithmetic(OpCodeAdd)
	i4 := NewInstruction().ChangeImmediateValue(4)
	i5 := NewInstruction().ChangeToArithmetic(OpCodeMul)
	// i6 := NewInstruction().ChangeImmediateValue(0)
	// i7 := NewInstruction().ChangeToArithmetic(OpCodeDiv)
	// i8 := NewInstruction().ChangeImmediateValue(8)
	// i9 := NewInstruction().ChangeToArithmetic(OpCodeSub)

	ins := []Instruction{i1, i2, i3, i4, i5} // i6, i7, i8, i9,

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

	db := getTestDB().Debug()

	var err error
	defer func() {
		r := recover()
		fmt.Printf("recover: %v\n", r)
		cleanSQL := "DELETE FROM test_users WHERE id IN (1, 2)"
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
