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
	GRPCOption    []grpc.DialOption
	TargetAddress string

	ctx context.Context
	cc  *grpc.ClientConn
	c   ServerServiceClient
}

func NewClient(options ...ClientOption) (*Client, error) {
	c := &Client{
		TargetAddress: "127.0.0.1:9527",
		ctx:           context.Background(),
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

type RegisterExecutorParams struct {
	ID     ExecutorID
	Host   Host
	Labels Labels
}

type UnregisterExecutorParams struct {
	ID    ExecutorID
	Error error
}

func (c *Client) RegisterExecutor(ctx context.Context, r RegisterExecutorParams, options ...ClientCallOption) error {
	req := &RegisterExecutorRequest{
		Id:   r.ID.String(),
		Host: r.Host.String(),
	}
	for _, v := range r.Labels {
		req.Labels = append(req.Labels, &RegisterExecutorRequest_Label{Key: v.Key, Value: v.Value})
	}
	calls := ClientCallOptions(options).GRPCOption()
	_, err := c.c.RegisterExecutor(ctx, req, calls...)
	return errors.Wrap(err, "Register Executor")
}

func (c *Client) UnregisterExecutor(ctx context.Context, r UnregisterExecutorParams, options ...ClientCallOption) error {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	req := &UnregisterExecutorRequest{
		Id:    r.ID.String(),
		Error: error,
	}
	calls := ClientCallOptions(options).GRPCOption()
	_, err := c.c.UnregisterExecutor(ctx, req, calls...)
	return errors.Wrap(err, "Unregister Executor")
}

func (c *Client) LoopCommands(ctx context.Context, options ...ClientCallOption) error {
	calls := ClientCallOptions(options).GRPCOption()

	stream, err := c.c.ExecuteCommand(ctx, calls...)
	if err != nil {
		return errors.Wrap(err, "Client execute command")
	}

	r := &CommandFromClient{
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
			ins := append([]*CommandToClient_Instruction{}, r2.ExecuteInstruction.Op)
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

			handler, err := NewInstructionSet().GetExecutorHandler(op.OpCode)
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

			r3 := &CommandFromClient{
				Id:   r2.Id,
				Type: CommandType_ExecuteInstruction,
				ExecuteInstruction: &CommandFromClient_ExecuteInstruction{
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

func (c *Client) Close() error {
	if c.cc != nil {
		err := c.cc.Close()
		c.cc = nil
		return err
	}
	return nil
}
