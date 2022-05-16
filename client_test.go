package gotlin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGotlin_StartComputeNode(t *testing.T) {
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

	p, ins := getTestProgram(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	c, err := NewClient(WithClientGRPCOptions(grpc.WithInsecure()))
	require.Nil(t, err)

	option := RegisterExecutorOption{
		ID:     NewExecutorID(),
		Host:   "127.0.0.1:0",
		Labels: NewLabels().Add(NewDefaultOpCodeLabel()),
	}
	err = c.RegisterExecutor(ctx, option)
	require.Nil(t, err)

	go func() {
		_ = c.StartComputeNode(ctx, StartComputeNodeOption{})
	}()

	time.Sleep(100 * time.Millisecond)

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	time.Sleep(1000 * time.Millisecond)

	result, err := g.QueryResult(ctx, p)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}

func TestGotlin_ClientSubmitProgramToExecute(t *testing.T) {
	time.Sleep(time.Second)

	ctx := context.Background()

	db := getTestDB()

	g, err := NewGotlin(WithDatabase(db), WithServerExecutor(true), WithEnableServer(true))
	require.Nil(t, err)

	go func() {
		err := g.StartServer(ctx)
		if err != nil {
			panic(err)
		}
	}()
	defer g.StopServer(false)

	time.Sleep(time.Second)

	p, ins := getTestProgram(t)

	c, err := NewClient(WithClientGRPCOptions(grpc.WithInsecure()))
	require.Nil(t, err)

	s, err := c.RequestScheduler(ctx, RequestSchedulerOption{})
	require.Nil(t, err)

	err = c.RunProgram(ctx, RunProgramOption{SchedulerID: s, Program: p, Instructions: ins})
	require.Nil(t, err)

	ch, err := c.WaitResult(context.Background())
	require.Nil(t, err)

	result := ProgramResult{}
	select {
	case <-time.After(time.Second):
	case result = <-ch:
	}

	require.Nil(t, result.Error)
	value := 0
	_ = json.Unmarshal(result.Result.([]byte), &value)
	assertProgramExecuteResult(t, 12, value)
}

func TestGotlin_ComputeNode_ExecuteConcurrently(t *testing.T) {
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

	p, ins := getTestProgram3(t)

	s, err := g.RequestScheduler(ctx, NewSchedulerOption())
	require.Nil(t, err)

	c, err := NewClient(WithClientGRPCOptions(grpc.WithInsecure()))
	require.Nil(t, err)

	option := RegisterExecutorOption{
		ID:     NewExecutorID(),
		Host:   "127.0.0.1:0",
		Labels: NewLabels().Add(NewDefaultOpCodeLabel()),
	}
	err = c.RegisterExecutor(ctx, option)
	require.Nil(t, err)

	go func() {
		_ = c.StartComputeNode(ctx, StartComputeNodeOption{})
	}()

	time.Sleep(100 * time.Millisecond)

	err = g.RunProgram(ctx, s, p, ins)
	require.Nil(t, err)

	time.Sleep(1000 * time.Millisecond)

	result, err := g.QueryResult(ctx, p)
	require.Nil(t, err)

	assertProgramExecuteResult(t, 12, result)
}
