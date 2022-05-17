package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	. "github.com/yaoguais/gotlin" //revive:disable-line
	"google.golang.org/grpc"
)

var (
	output  = fmt.Print
	outputf = fmt.Printf
)

func main() {
	err := getApp().Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getApp() *cli.App {
	app := &cli.App{
		Name:  "gotlin",
		Usage: "a DAG-based distributed task engine for massively parallel computing",
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start a service node",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "address",
						Aliases: []string{"p"},
						Usage:   "The listening address has the format host:port",
						Value:   "0.0.0.0:9527",
					},
					&cli.BoolFlag{
						Name:    "executor",
						Aliases: []string{"e"},
						Usage:   "Whether to use this service node as a computing node",
						Value:   true,
					},
					&cli.StringFlag{
						Name:    "driver",
						Aliases: []string{"d"},
						Usage:   "Database drivers such as mysql, postgres, clickhouse, use an in-memory database by default",
						Value:   "mysql",
					},
					&cli.StringFlag{
						Name:    "dsn",
						Aliases: []string{"n"},
						Usage:   "The data source name for the database connection string",
						EnvVars: []string{"DATABASE_DSN"},
						Value:   "",
					},
				},
				Action: start,
			},
			{
				Name:  "compute",
				Usage: "Start a compute node",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server",
						Aliases: []string{"s"},
						Usage:   "The address of the service node has the format host:port",
						Value:   "127.0.0.1:9527",
					},
					&cli.StringFlag{
						Name:    "id",
						Aliases: []string{"i"},
						Usage:   "The unique identifier of this computing node",
						Value:   NewExecutorID().String(),
					},
				},
				Action: compute,
			},
			{
				Name:  "submit",
				Usage: "Submit a task to service node",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server",
						Aliases: []string{"s"},
						Usage:   "The address of the service node has the format host:port",
						Value:   "127.0.0.1:9527",
					},
					&cli.StringFlag{
						Name:    "program",
						Aliases: []string{"p"},
						Usage:   "Submit the task and use the @ symbol to read from the file",
						Value:   "@program.json",
					},
				},

				Action: submit,
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	return app
}

func start(c *cli.Context) error {
	address := c.String("address")
	executor := c.Bool("executor")
	driver := c.String("driver")
	dsn := c.String("dsn")

	options := []Option{
		WithServerAddress(address),
		WithServerExecutor(executor),
		WithEnableServer(true),
	}

	if dsn != "" {
		db, err := DatabaseFactory(driver, dsn)
		if err != nil {
			return err
		}
		options = append(options, WithDatabase(db))
	}

	g, err := NewGotlin(options...)
	if err != nil {
		return err
	}

	errCh := make(chan error)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGKILL, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := g.StartServer(ctx)
		errCh <- err
	}()

	select {
	case s := <-ch:
		return g.StopServer(s == syscall.SIGTERM)
	case err := <-errCh:
		_ = g.StopServer(false)
		return err
	}
}

func compute(c *cli.Context) error {
	server := c.String("server")
	id := c.String("id")

	executorID, err := ParseExecutorID(id)
	if err != nil {
		return err
	}

	options := []ClientOption{
		WithClientTargetAddress(server),
		WithClientGRPCOptions(grpc.WithInsecure()),
	}

	g, err := NewClient(options...)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = g.RegisterExecutor(ctx, RegisterExecutorOption{
		ID:   executorID,
		Host: EmptyHost,
	})
	if err != nil {
		return err
	}
	defer g.UnregisterExecutor(context.Background(), UnregisterExecutorOption{ID: executorID})

	errCh := make(chan error)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGKILL, syscall.SIGTERM)

	go func() {
		err := g.StartComputeNode(ctx, StartComputeNodeOption{})
		errCh <- err
	}()

	select {
	case <-ch:
		return g.Shutdown()
	case err := <-errCh:
		_ = g.Shutdown()
		return err
	}
}

func submit(c *cli.Context) error {
	server := c.String("server")
	file := c.String("program")

	p, ins, err := parseProgramFile(strings.TrimLeft(file, "@"))
	if err != nil {
		return err
	}

	options := []ClientOption{
		WithClientTargetAddress(server),
		WithClientGRPCOptions(grpc.WithInsecure()),
	}

	g, err := NewClient(options...)
	if err != nil {
		return err
	}

	ctx := context.Background()

	s, err := g.RequestScheduler(ctx, RequestSchedulerOption{})
	if err != nil {
		return err
	}

	err = g.RunProgram(ctx, RunProgramOption{
		SchedulerID:  s,
		Program:      p,
		Instructions: ins,
	})
	if err != nil {
		return err
	}

	ch, err := g.WaitResult(ctx, WaitResultOption{IDs: []ProgramID{p.ID}})
	if err != nil {
		return err
	}

	for v := range ch {
		if v.ID != p.ID {
			continue
		}
		if v.Error != nil {
			return errors.Errorf("Error in program, %v\n", v.Error)
		}
		var value interface{}
		err = json.Unmarshal(v.Result.([]byte), &value)
		if err != nil {
			return err
		}
		outputf("Program evaluates to %v\n", value)
		return nil
	}

	return nil
}

func parseProgramFile(file string) (p Program, ins []Instructioner, err error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}
	return parseProgram(data)
}

func parseProgram(data []byte) (p Program, ins []Instructioner, err error) {
	input := struct {
		ID   string `json:"id"`
		Code []struct {
			ID      string  `json:"id"`
			OpCode  string  `json:"opcode"`
			Operand Operand `json:"operand"`
		} `json:"code"`
		Processor struct {
			Core int `json:"core"`
			DAG  []struct {
				Partent  string   `json:"parent"`
				Children []string `json:"children"`
			} `json:"dag"`
		} `json:"processor"`
	}{}

	err = json.Unmarshal(data, &input)
	if err != nil {
		return
	}

	programID := NewProgramID()
	if v, err := ParseProgramID(input.ID); err == nil {
		programID = v
	}

	m := make(map[string]InstructionID)
	for _, v := range input.Code {
		i1, i2 := parseIDPairs(v.ID)
		m[i2] = i1
	}

	for _, v := range input.Code {
		in := NewInstruction()
		in.ID = m[v.ID]
		in.OpCode = OpCode(v.OpCode)
		in.Operand = v.Operand
		ins = append(ins, in)
	}

	p = NewProgram()
	p.ID = programID

	for _, in := range ins {
		p.AddInstruction(in.Instruction().ID)
	}

	d := NewInstructionDAG()

	ids := []InstructionID{}
	for _, v := range ins {
		ids = append(ids, v.Instruction().ID)
	}
	err = d.Add(ids...)
	if err != nil {
		return
	}

	for _, v := range input.Processor.DAG {
		pid := m[v.Partent]
		cid := []InstructionID{}
		for _, v2 := range v.Children {
			cid = append(cid, m[v2])
		}
		err = d.AttachChildren(pid, cid...)
		if err != nil {
			return
		}
	}

	p = p.ChangeProcessor(NewDAGProcessorContext(d, input.Processor.Core))
	p, _ = p.ChangeState(StateReady)

	return
}

func parseIDPairs(s string) (InstructionID, string) {
	id, err := ParseInstructionID(s)
	if err != nil {
		return NewInstructionID(), s
	}
	return id, s
}
