package gotlin

import (
	"context"
	"encoding/json"
	"io"

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
	req := pbConverter{}.RegisterExecutorOptionToPb(r)
	_, err := c.c.RegisterExecutor(ctx, req, r.CallOptions.GRPCOption()...)
	return errors.Wrap(err, "Register Executor")
}

func (c *Client) UnregisterExecutor(ctx context.Context, r UnregisterExecutorOption) error {
	req := pbConverter{}.UnregisterExecutorOptionToPb(r)
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
	SchedulerOption
	CallOptions ClientCallOptions
}

func (c *Client) RequestScheduler(ctx context.Context, r RequestSchedulerOption) (SchedulerID, error) {
	req := pbConverter{}.RequestSchedulerOptionToPb(r)
	resp, err := c.c.RequestScheduler(ctx, req, r.CallOptions.GRPCOption()...)
	if err != nil {
		return SchedulerID{}, err
	}
	return ParseSchedulerID(resp.GetId())
}

type RunProgramOption struct {
	SchedulerID  SchedulerID
	Program      Program
	Instructions []Instructioner
	CallOptions  ClientCallOptions
}

func (c *Client) RunProgram(ctx context.Context, r RunProgramOption) error {
	req := pbConverter{}.RunProgramOptionToPb(r)
	_, err := c.c.RunProgram(ctx, req, r.CallOptions.GRPCOption()...)
	return err
}

func (c *Client) WaitResult(ctx context.Context) (chan ProgramResult, error) {
	stream, err := c.c.WaitResult(ctx, &WaitResultRequest{})
	if err != nil {
		return nil, err
	}

	ch := make(chan ProgramResult)

	go func() {
		defer close(ch)

		pc := pbConverter{}

		for {
			select {
			case <-ctx.Done():
				ch <- ProgramResult{Error: errors.Wrap(ctx.Err(), "From Client")}
				return
			default:
			}

			m, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err == context.Canceled {
				ch <- ProgramResult{Error: errors.Wrap(err, "From Server")}
				return
			} else if err != nil {
				ch <- ProgramResult{Error: errors.Wrap(ErrProgramExitUnexpectedly, err.Error())}
				return
			}
			ch <- pc.WaitResultResponseToModel(m)
		}
	}()

	return ch, nil
}

func (c *Client) Shutdown() error {
	if c.cc != nil {
		err := c.cc.Close()
		c.cc = nil
		return err
	}
	return nil
}

type pbConverter struct {
}

func (pbConverter) RegisterExecutorOptionToPb(r RegisterExecutorOption) *RegisterExecutorRequest {
	req := &RegisterExecutorRequest{
		Id:   r.ID.String(),
		Host: r.Host.String(),
	}
	for _, v := range r.Labels {
		req.Labels = append(req.Labels, &RegisterExecutorRequest_Label{Key: v.Key, Value: v.Value})
	}
	return req
}

func (pbConverter) UnregisterExecutorOptionToPb(r UnregisterExecutorOption) *UnregisterExecutorRequest {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	req := &UnregisterExecutorRequest{
		Id:    r.ID.String(),
		Error: error,
	}
	return req
}

func (pbConverter) RequestSchedulerOptionToPb(r RequestSchedulerOption) *RequestSchedulerRequest {
	req := &RequestSchedulerRequest{
		Dummy: r.Dummy,
	}
	return req
}

func (pbConverter) RequestSchedulerRequestToModel(r *RequestSchedulerRequest) SchedulerOption {
	return SchedulerOption{
		Dummy: r.Dummy,
	}
}

func (pbConverter) ProgramToPb(r Program) *ProgramPb {
	code, _ := json.Marshal(r.Code)
	processor, _ := json.Marshal(r.Processor)
	return &ProgramPb{
		Id:        r.ID.String(),
		Code:      code,
		Processor: processor,
	}
}

func (pbConverter) ProgramToModel(r *ProgramPb) Program {
	p := NewProgram()

	id, _ := ParseProgramID(r.Id)
	p.ID = id

	code := ProgramCode{}
	_ = json.Unmarshal(r.Code, &code)
	p.Code = code

	processor := ProcessorContext{}
	_ = json.Unmarshal(r.Processor, &processor)
	p.Processor = processor

	p, _ = p.ChangeState(StateReady)
	return p
}

func (pbConverter) InstructionerToPb(r Instructioner) *InstructionPb {
	in := r.Instruction()
	_, isRef := r.(InstructionRefer)
	operand, _ := json.Marshal(in.Operand)
	result, _ := json.Marshal(in.Result)
	req := &InstructionPb{
		Id:      in.ID.String(),
		Opcode:  in.OpCode.String(),
		Operand: operand,
		Result:  result,
	}
	if isRef {
		id2, _ := json.Marshal(in.ID)
		req.Id2 = string(id2)
	}
	return req
}

func (pbConverter) InstructionToModel(r *InstructionPb) Instructioner {
	id, _ := ParseInstructionID(r.Id)

	in := NewInstruction()
	in.ID = id
	in.OpCode = OpCode(r.Opcode)

	operand := Operand{}
	_ = json.Unmarshal(r.Operand, &operand)
	in.Operand = operand

	result := InstructionResult{}
	_ = json.Unmarshal(r.Result, &result)
	in.Result = result

	if r.Id2 == "" {
		return in
	}

	var id2 InstructionID
	_ = json.Unmarshal([]byte(r.Id2), &id2)
	in.ID = id2
	return in

}

func (c pbConverter) InstructionersToPb(rs []Instructioner) []*InstructionPb {
	req := make([]*InstructionPb, 0, len(rs))
	for _, r := range rs {
		req = append(req, c.InstructionerToPb(r))
	}
	return req
}

func (c pbConverter) InstructionToModels(rs []*InstructionPb) []Instructioner {
	ms := make([]Instructioner, 0, len(rs))
	for _, r := range rs {
		ms = append(ms, c.InstructionToModel(r))
	}
	return ms
}

func (c pbConverter) RunProgramOptionToPb(r RunProgramOption) *RunProgramRequest {
	req := &RunProgramRequest{
		SchedulerId:  r.SchedulerID.String(),
		Program:      c.ProgramToPb(r.Program),
		Instructions: c.InstructionersToPb(r.Instructions),
	}
	return req
}

func (pbConverter) WaitResultResponseToModel(r *WaitResultResponse) ProgramResult {
	id, _ := ParseProgramID(r.GetId())
	var err error
	if r.Error != "" {
		err = errors.New(r.Error)
	}
	return ProgramResult{
		ID:     id,
		Result: r.Result,
		Error:  err,
	}
}

func (pbConverter) WaitResultResponseFromModel(r ProgramResult) *WaitResultResponse {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	result, _ := json.Marshal(r.Result)

	return &WaitResultResponse{
		Id:     r.ID.String(),
		Error:  error,
		Result: result,
	}
}
