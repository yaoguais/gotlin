package gotlin

import (
	"context"
	"encoding/json"
	"io"

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
	formatError
}

func NewClient(options ...ClientOption) (*Client, error) {
	e := formatError{"Client " + NewClientID().String() + " %s"}
	c := &Client{
		TargetAddress:  "127.0.0.1:9527",
		InstructionSet: NewInstructionSet(),
		ctx:            context.Background(),
		formatError:    e,
	}

	for _, o := range options {
		o(c)
	}

	cc, err := grpc.DialContext(c.ctx, c.TargetAddress, c.GRPCOption...)
	if err != nil {
		return nil, c.error(ErrConnect, err, "Connect to server")
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
	return c.error(ErrRequest, err, "Register Executor")
}

func (c *Client) UnregisterExecutor(ctx context.Context, r UnregisterExecutorOption) error {
	req := pbConverter{}.UnregisterExecutorOptionToPb(r)
	_, err := c.c.UnregisterExecutor(ctx, req, r.CallOptions.GRPCOption()...)
	return c.error(ErrRequest, err, "Unregister Executor")
}

type StartComputeNodeOption struct {
	CallOptions ClientCallOptions
}

func (c *Client) StartComputeNode(ctx context.Context, r StartComputeNodeOption) error {
	return c.execute(ctx, r.CallOptions)
}

func (c *Client) execute(ctx context.Context, callOptions ClientCallOptions) error {
	stream, err := c.c.Execute(ctx, callOptions.GRPCOption()...)
	if err != nil {
		return c.error(ErrRequest, err, "Client execute command")
	}

	r := &ExecuteStream{
		Type: ExecuteStream_Connect,
	}
	err = stream.Send(r)
	if err != nil {
		return c.error(ErrRequest, err, "Client connect to server")
	}

	pc := pbConverter{}

	for {

		r2, err := stream.Recv()
		if err != nil {
			return c.error(ErrRequest, err, "Client receive from server")
		}

		println("client receive ==> ", r2.String())

		if r2.Type == ExecuteStream_Execute {
			r3, err := pc.ParseExecuteStream(r2)
			if err != nil {
				return err
			}

			op := r3.(executeStreamExecuteRequest).Op
			args := r3.(executeStreamExecuteRequest).Args
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
				return c.error(ErrRequest, err, "Marshal remote result")
			}

			r4 := &ExecuteStream{
				Id:   r2.Id,
				Type: ExecuteStream_Result,
				Result: &InstructionPb{
					Id:     op.ID.String(),
					Opcode: op.OpCode.String(),
					Result: data,
				},
			}

			println("client send ==> ", r4.String())

			err = stream.Send(r4)
			if err != nil {
				return c.error(ErrRequest, err, "Client send Instruction execute result")
			}
		} else {
			return c.error(ErrRequest, ErrUndoubted, "Client receive invalid type "+r2.Type.String())
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
				ch <- ProgramResult{Error: c.error(ctx.Err(), ErrUndoubted, "From Client")}
				return
			default:
			}

			m, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err == context.Canceled {
				ch <- ProgramResult{Error: c.error(context.Canceled, ErrUndoubted, "From Server")}
				return
			} else if err != nil {
				ch <- ProgramResult{Error: c.error(ErrExitUnexpectedly, err, "Program exit")}
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
		Id: r.ID.String(),
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
		err = newError(r.Error)
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

func (c pbConverter) ParseExecuteStream(r *ExecuteStream) (interface{}, error) {
	switch r.Type {
	case ExecuteStream_Connect:
		return c.ParseExecuteStreamConnect(r)
	case ExecuteStream_Execute:
		return c.ParseExecuteStreamExecute(r)
	case ExecuteStream_Result:
		return c.ParseExecuteStreamResult(r)
	default:
		return nil, wrapError(ErrRequest, "Client receive invalid type %s", r.Type)
	}
}

func (pbConverter) ParseExecuteStreamConnect(r *ExecuteStream) (interface{}, error) {
	return struct{}{}, nil
}

type executeStreamExecuteRequest struct {
	Op   Instruction
	Args []Instruction
}

func (c pbConverter) ParseExecuteStreamExecute(r *ExecuteStream) (interface{}, error) {
	result := executeStreamExecuteRequest{}
	result.Op = c.InstructionToModel(r.Op).Instruction()
	for _, v := range r.Args {
		result.Args = append(result.Args, c.InstructionToModel(v).Instruction())
	}
	return result, nil
}

func (c pbConverter) ParseExecuteStreamResult(r *ExecuteStream) (interface{}, error) {
	return c.InstructionToModel(r.Result), nil
}
