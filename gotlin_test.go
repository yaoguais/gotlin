package gotlin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestGotlin(t *testing.T) {
	err := run()
	require.Nil(t, err)
}

func run() error {
	ctx := context.Background()

	dsn := "root:ca18ab7be64df8343b648c28591bca1a@tcp(127.0.0.1:3306)/gotlin?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	db = db.Debug()

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

	p, _ = p.ChangeState(StateReady)

	err = g.LoadScheduler(ctx, s)
	if err != nil {
		return err
	}

	err = g.LoadProgram(ctx, p, ins)
	if err != nil {
		return err
	}

	return g.AssignScheduler(ctx, s, p)
}
