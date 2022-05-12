package gotlin

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type ClientOption func(*Client)

func WithClientTargetAddress(addr string) ClientOption {
	return func(c *Client) {
		c.TargetAddress = addr
	}
}

func WithClientGRPCOptions(options ...grpc.DialOption) ClientOption {
	return func(c *Client) {
		c.GRPCOption = options
	}
}

func WithClientInstructionSet(is *InstructionSet) ClientOption {
	return func(c *Client) {
		c.InstructionSet = is
	}
}

type ClientCallOption struct {
	GRPCOption []grpc.CallOption
}

type ClientCallOptions []ClientCallOption

func (o ClientCallOptions) GRPCOption() []grpc.CallOption {
	n := len(o)
	if n > 1 {
		panic("Only supports passing in one ClientCallOption parameter")
	} else if n == 0 {
		return []grpc.CallOption{}
	}
	return o[0].GRPCOption
}

type Client struct {
	GRPCOption     []grpc.DialOption
	TargetAddress  string
	InstructionSet *InstructionSet

	ctx context.Context
	cc  *grpc.ClientConn
	c   ServerServiceClient
}

func NewClient(options ...ClientOption) (*Client, error) {
	c := &Client{
		TargetAddress:  "127.0.0.1:9527",
		InstructionSet: NewInstructionSet(),
		ctx:            context.Background(),
	}

	for _, o := range options {
		o(c)
	}

	cc, err := grpc.DialContext(c.ctx, c.TargetAddress, c.GRPCOption...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect to server")
	}
	c.cc = cc
	c.c = NewServerServiceClient(cc)

	return c, nil
}

type RegisterExecutorOption struct {
	ID          ExecutorID
	Host        Host
	Labels      Labels
	CallOptions ClientCallOptions
}

type UnregisterExecutorOption struct {
	ID          ExecutorID
	Error       error
	CallOptions ClientCallOptions
}

func (c *Client) RegisterExecutor(ctx context.Context, r RegisterExecutorOption) error {
	req := &RegisterExecutorRequest{
		Id:   r.ID.String(),
		Host: r.Host.String(),
	}
	for _, v := range r.Labels {
		req.Labels = append(req.Labels, &RegisterExecutorRequest_Label{Key: v.Key, Value: v.Value})
	}
	_, err := c.c.RegisterExecutor(ctx, req, r.CallOptions.GRPCOption()...)
	return errors.Wrap(err, "Register Executor")
}

func (c *Client) UnregisterExecutor(ctx context.Context, r UnregisterExecutorOption) error {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	req := &UnregisterExecutorRequest{
		Id:    r.ID.String(),
		Error: error,
	}
	_, err := c.c.UnregisterExecutor(ctx, req, r.CallOptions.GRPCOption()...)
	return errors.Wrap(err, "Unregister Executor")
}

type StartComputeNodeOption struct {
	CallOptions ClientCallOptions
}

func (c *Client) StartComputeNode(ctx context.Context, r StartComputeNodeOption) error {
	return c.handleInstructions(ctx, r.CallOptions)
}

func (c *Client) handleInstructions(ctx context.Context, callOptions ClientCallOptions) error {
	stream, err := c.c.ExecuteCommand(ctx, callOptions.GRPCOption()...)
	if err != nil {
		return errors.Wrap(err, "Client execute command")
	}

	r := &CommandFromRemote{
		Type: CommandType_ConnectToServer,
	}
	err = stream.Send(r)
	if err != nil {
		return errors.Wrap(err, "Client connect to server")
	}

	for {

		r2, err := stream.Recv()
		if err != nil {
			return errors.Wrap(err, "Client receive from server")
		}

		println("client receive ==> ", r2.String())

		if r2.Type == CommandType_ExecuteInstruction {
			ts := []Instruction{}
			ins := append([]*CommandToRemote_Instruction{}, r2.ExecuteInstruction.Op)
			ins = append(ins, r2.ExecuteInstruction.Args...)
			for i, in := range ins {
				id, err := ParseInstructionID(in.Id)
				if err != nil {
					return errors.Wrapf(err, "Client parse instruction id %d", i)
				}
				var operand Operand
				err = json.Unmarshal(in.GetOperand(), &operand)
				if err != nil {
					return errors.Wrapf(err, "Client unmarshal operand %d", i)
				}
				var result InstructionResult
				err = json.Unmarshal(in.GetResult(), &result)
				if err != nil {
					return errors.Wrapf(err, "Client unmarshal operand %d", i)
				}
				t := Instruction{
					ID:      id,
					OpCode:  OpCode(in.GetOpcode()),
					Operand: operand,
					Result:  result,
				}
				ts = append(ts, t)
			}

			op := ts[0]
			args := ts[1:]

			handler, err := c.InstructionSet.GetExecutorHandler(op.OpCode)
			if err != nil {
				return err
			}
			result, err := handler(ctx, op, args...)
			if err != nil {
				return err
			}
			data, err := json.Marshal(result)
			if err != nil {
				return errors.Wrap(err, "Marshal remote result")
			}

			r3 := &CommandFromRemote{
				Id:   r2.Id,
				Type: CommandType_ExecuteInstruction,
				ExecuteInstruction: &CommandFromRemote_ExecuteInstruction{
					Id:     r2.ExecuteInstruction.GetOp().GetId(),
					Opcode: r2.ExecuteInstruction.GetOp().GetOpcode(),
					Result: data,
				},
			}

			println("client send ==> ", r3.String())

			err = stream.Send(r3)
			if err != nil {
				return errors.Wrap(err, "Client send Instruction execute result")
			}
		} else {
			return errors.Errorf("Client receive invalid type %s", r2.Type)
		}
	}
}

type RequestSchedulerOption struct {
	CallOptions ClientCallOptions
}

type RequestSchedulerResult struct {
	SchedulerID SchedulerID
}

func (c *Client) RequestScheduler(ctx context.Context, r RequestSchedulerOption) (RequestSchedulerResult, error) {
	return RequestSchedulerResult{}, errors.New("Unimplemented")
}

type RunProgramOption struct {
	SchedulerID  SchedulerID
	Program      Program
	Instructions []Instruction
	CallOptions  ClientCallOptions
}

type RunProgramResult struct {
}

func (c *Client) RunProgram(ctx context.Context, r RunProgramOption) (RunProgramResult, error) {
	return RunProgramResult{}, errors.New("Unimplemented")
}

type ProgramResult struct {
	ID     ProgramID
	Result interface{}
	Error  error
}

func (c *Client) WaitResult(ctx context.Context) (chan ProgramResult, chan error) {
	errCh := make(chan error, 1)
	errCh <- errors.New("Unimplemented")
	return nil, errCh
}

func (c *Client) Shutdown() error {
	if c.cc != nil {
		err := c.cc.Close()
		c.cc = nil
		return err
	}
	return nil
}
