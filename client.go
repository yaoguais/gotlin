package gotlin

import (
	"context"
	"encoding/json"
	"io"
	"net"

	. "github.com/yaoguais/gotlin/proto" //revive:disable-line
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

type Client struct {
	GRPCOption     []grpc.DialOption
	TargetAddress  string
	InstructionSet *InstructionSet

	ctx context.Context
	cc  *grpc.ClientConn
	c   ServerServiceClient
	id  string
	l   clientLogger
	formatError
}

func NewClient(options ...ClientOption) (*Client, error) {
	id := NewClientID().String()
	c := &Client{
		TargetAddress:  "127.0.0.1:9527",
		InstructionSet: NewInstructionSet(),
		ctx:            context.Background(),
		id:             id,
		l:              clientLogger{}.WithClient(id),
		formatError:    formatError{"Client " + id + " %s"},
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
	CallOptions []grpc.CallOption
}

type UnregisterExecutorOption struct {
	ID          ExecutorID
	Error       error
	CallOptions []grpc.CallOption
}

func (c *Client) RegisterExecutor(ctx context.Context, r RegisterExecutorOption) error {
	r.Labels = r.Labels.Add(c.InstructionSet.OpCodeLabel())
	req, err := pbConverter{}.RegisterExecutorOptionToPb(r)
	if err != nil {
		return c.error(ErrConverter, err, "RegisterExecutorOption")
	}
	_, err = c.c.RegisterExecutor(ctx, req, r.CallOptions...)
	return c.error(ErrRequest, err, "Register Executor")
}

func (c *Client) UnregisterExecutor(ctx context.Context, r UnregisterExecutorOption) error {
	req, err := pbConverter{}.UnregisterExecutorOptionToPb(r)
	if err != nil {
		return c.error(ErrConverter, err, "UnregisterExecutorOption")
	}
	_, err = c.c.UnregisterExecutor(ctx, req, r.CallOptions...)
	return c.error(ErrRequest, err, "Unregister Executor")
}

type StartComputeNodeOption struct {
	CallOptions []grpc.CallOption
}

func (c *Client) StartComputeNode(ctx context.Context, r StartComputeNodeOption) error {
	return c.executeLoop(ctx, r.CallOptions)
}

func (c *Client) executeLoop(ctx context.Context, callOptions []grpc.CallOption) error {
	logger := c.l.Logger()
	logger.Info("The compute node is connecting to the server")

	stream, err := c.c.Execute(ctx, callOptions...)
	if err != nil {
		return c.error(ErrRequest, err, "Client execute instructions")
	}

	r := &ExecuteStream{Id: NewExecuteID().String(), Type: ExecuteStream_Connect}
	err = stream.Send(r)
	if err != nil {
		return c.error(ErrRequest, err, "Client connect to server")
	}

	l := c.l.WithExecuteID(r.Id)
	logger = l.Logger()
	logger.Debugf("Send a connection instruction, %s", r)

	logger.Info("The compute node has successfully connected to the server")
	defer logger.Info("The compute node is disconnecting from the server")

	for {
		err := c.execute(stream)
		if err != nil {
			logger.Errorf("The compute node execution error, %v", err)
			return err
		}
	}
}

func (c *Client) execute(stream ServerService_ExecuteClient) error {
	pc := pbConverter{}

	r, err := stream.Recv()
	if err != nil {
		return c.error(ErrRequest, err, "Client receive from server")
	}

	l := c.l.WithExecuteID(r.Id)
	logger := l.Logger()
	logger.Debugf("Received an instruction, %s", r)

	if r.Type != ExecuteStream_Execute {
		return c.error(ErrRequest, ErrUndoubted, "Client receive invalid type "+r.Type.String())
	}

	e, err := pc.ParseExecuteStream(r)
	if err != nil {
		return err
	}

	op := e.(executeStreamExecuteRequest).Op
	args := e.(executeStreamExecuteRequest).Args

	logger = l.WithExecute(op, args).Logger()
	logger.Info("Preparing to execute an instruction")

	handler, err := c.InstructionSet.GetExecutorHandler(op.OpCode)
	if err != nil {
		return err
	}

	return c.executeAsync(stream, handler, op, args, r, logger)
}

func (c *Client) executeAsync(stream ServerService_ExecuteClient,
	handler ExecutorHandler, op Instruction, args []Instruction, rr *ExecuteStream, logger Logger) error {

	// TODO fix max gotoutines limits
	go func() error {
		ctx := stream.Context()
		result, err := handler(ctx, op, args...)
		if err != nil {
			logger.Warn("An exception occurred while executing an instruction")
		}
		err = c.sendResult(stream, rr, result, err, logger)
		if err == nil {
			logger.Info("Instruction was executed successfully")
		}
		return err
	}()

	return nil
}

func (c *Client) sendResult(stream ServerService_ExecuteClient,
	rr *ExecuteStream, result InstructionResult, handleErr error, logger Logger) error {
	var error string
	if handleErr != nil {
		error = handleErr.Error()
		result = NewEmptyInstructionResult()
	}

	data, err := marshal(result)
	if err != nil {
		return c.error(ErrRequest, err, "Marshal remote result")
	}

	r := &ExecuteStream{
		Id:    rr.Id,
		Type:  ExecuteStream_Result,
		Error: error,
		Result: &InstructionPb{
			Id:      rr.Op.Id,
			Opcode:  rr.Op.Opcode,
			Operand: rr.Op.Operand,
			Result:  data,
		},
	}
	logger.Debugf("Send the result of an instruction, %s", r)
	err = stream.Send(r)
	return c.error(ErrRequest, err, "Client send Instruction execute result")
}

type RequestSchedulerOption struct {
	SchedulerOption
	CallOptions []grpc.CallOption
}

func (c *Client) RequestScheduler(ctx context.Context, r RequestSchedulerOption) (SchedulerID, error) {
	req, err := pbConverter{}.RequestSchedulerOptionToPb(r)
	if err != nil {
		return SchedulerID{}, c.error(ErrConverter, err, "RequestSchedulerOption")
	}
	resp, err := c.c.RequestScheduler(ctx, req, r.CallOptions...)
	if err != nil {
		return SchedulerID{}, err
	}
	return ParseSchedulerID(resp.GetId())
}

type RunProgramOption struct {
	SchedulerID  SchedulerID
	Program      Program
	Instructions []Instructioner
	CallOptions  []grpc.CallOption
}

func (c *Client) RunProgram(ctx context.Context, r RunProgramOption) error {
	req, err := pbConverter{}.RunProgramOptionToPb(r)
	if err != nil {
		return c.error(ErrConverter, err, "RunProgramOption")
	}
	_, err = c.c.RunProgram(ctx, req, r.CallOptions...)
	return err
}

type WaitResultOption struct {
	IDs         []ProgramID
	CallOptions []grpc.CallOption
}

func (c *Client) WaitResult(ctx context.Context, r WaitResultOption) (chan ProgramResult, error) {
	pc := pbConverter{}

	req, err := pc.WaitResultOptionToPb(r)
	if err != nil {
		return nil, c.error(ErrConverter, err, "WaitResultOption")
	}

	stream, err := c.c.WaitResult(ctx, req, r.CallOptions...)
	if err != nil {
		return nil, err
	}

	ch := make(chan ProgramResult)

	go func() {
		defer close(ch)

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
			result, err := pc.WaitResultResponseToModel(m)
			if err != nil {
				ch <- ProgramResult{Error: c.error(ErrExitUnexpectedly, err, "Convert WaitResultResponse")}
				return
			}
			ch <- result
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

func (pbConverter) RegisterExecutorRequestToModel(r *RegisterExecutorRequest) (e Executor, err error) {
	id, err := ParseExecutorID(r.Id)
	if err != nil {
		return Executor{}, wrapError(err, "Parse Executor id")
	}
	_, _, err = net.SplitHostPort(r.Host)
	if err != nil {
		return Executor{}, wrapError(err, "Parse Host")
	}
	ls := []string{}
	for _, v := range r.Labels {
		ls = append(ls, v.Key)
		ls = append(ls, v.Value)
	}
	labels := NewLabels(ls...)
	_, ok := labels.Find(OpCodeLabelKey)
	if !ok {
		return Executor{}, newErrorf("Executor must have %s label", OpCodeLabelKey)
	}

	return Executor{
		ID:         id,
		Labels:     labels,
		Host:       Host(r.Host),
		State:      StateRunning,
		Error:      nil,
		Limits:     NewEmptyResource(),
		Usages:     NewEmptyResource(),
		CreateTime: NewTimestamp(),
		UpdateTime: NewTimestamp(),
		FinishTime: TimestampZero,
	}, nil
}

func (pbConverter) RegisterExecutorOptionToPb(r RegisterExecutorOption) (*RegisterExecutorRequest, error) {
	req := &RegisterExecutorRequest{Id: r.ID.String()}
	for _, v := range r.Labels {
		label := &RegisterExecutorRequest_Label{Key: v.Key, Value: v.Value}
		req.Labels = append(req.Labels, label)
	}
	return req, nil
}

func (pbConverter) UnregisterExecutorOptionToPb(r UnregisterExecutorOption) (*UnregisterExecutorRequest, error) {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	req := &UnregisterExecutorRequest{Id: r.ID.String(), Error: error}
	return req, nil
}

func (pbConverter) RequestSchedulerOptionToPb(r RequestSchedulerOption) (*RequestSchedulerRequest, error) {
	req := &RequestSchedulerRequest{Dummy: r.dummy}
	return req, nil
}

func (pbConverter) RequestSchedulerRequestToModel(r *RequestSchedulerRequest) (SchedulerOption, error) {
	if r == nil {
		return SchedulerOption{}, ErrNilPointer
	}
	return SchedulerOption{dummy: r.Dummy}, nil
}

func (pbConverter) ProgramToPb(r Program) (*ProgramPb, error) {
	code, err := json.Marshal(r.Code)
	if err != nil {
		return nil, err
	}
	processor, err := json.Marshal(r.Processor)
	if err != nil {
		return nil, err
	}
	p := &ProgramPb{
		Id:        r.ID.String(),
		Code:      code,
		Processor: processor,
	}
	return p, nil
}

func (pbConverter) ProgramToModel(r *ProgramPb) (p Program, err error) {
	if r == nil {
		return Program{}, ErrNilPointer
	}

	p = NewProgram()

	id, err := ParseProgramID(r.Id)
	if err != nil {
		return
	}
	p.ID = id

	code := ProgramCode{}
	err = json.Unmarshal(r.Code, &code)
	if err != nil {
		return
	}
	p.Code = code

	processor := ProcessorContext{}
	err = json.Unmarshal(r.Processor, &processor)
	if err != nil {
		return
	}
	p.Processor = processor

	p, ok := p.ChangeState(StateReady)
	if !ok {
		return Program{}, wrapError(ErrProgramState, "Not ready")
	}
	return p, nil
}

func (pbConverter) InstructionerToPb(r Instructioner) (*InstructionPb, error) {
	in := r.Instruction()
	_, isRef := r.(InstructionRefer)

	operand, err := marshal(in.Operand)
	if err != nil {
		return nil, err
	}

	result, err := marshal(in.Result)
	if err != nil {
		return nil, err
	}

	req := &InstructionPb{
		Id:      in.ID.String(),
		Opcode:  in.OpCode.String(),
		Operand: operand,
		Result:  result,
	}

	if isRef {
		id2, err := json.Marshal(in.ID)
		if err != nil {
			return nil, err
		}
		req.Id2 = string(id2)
	}

	return req, nil
}

func (pbConverter) InstructionToModel(r *InstructionPb) (Instructioner, error) {
	if r == nil {
		return nil, ErrNilPointer
	}

	id, err := ParseInstructionID(r.Id)
	if err != nil {
		return nil, err
	}

	in := NewInstruction()
	in.ID = id
	in.OpCode = OpCode(r.Opcode)

	operand := Operand{}
	err = unmarshal(r.Operand, &operand)
	if err != nil {
		return nil, err
	}
	in.Operand = operand

	result := InstructionResult{}
	err = unmarshal(r.Result, &result)
	if err != nil {
		return nil, err
	}
	in.Result = result

	if r.Id2 == "" {
		return in, nil
	}

	var id2 InstructionID
	err = json.Unmarshal([]byte(r.Id2), &id2)
	if err != nil {
		return nil, err
	}
	in.ID = id2
	return in, nil

}

func (c pbConverter) InstructionersToPb(rs []Instructioner) ([]*InstructionPb, error) {
	req := make([]*InstructionPb, 0, len(rs))
	for _, r := range rs {
		in, err := c.InstructionerToPb(r)
		if err != nil {
			return nil, err
		}
		req = append(req, in)
	}
	return req, nil
}

func (c pbConverter) InstructionToModels(rs []*InstructionPb) ([]Instructioner, error) {
	ms := make([]Instructioner, 0, len(rs))
	for _, r := range rs {
		if r == nil {
			return nil, ErrNilPointer
		}
		m, err := c.InstructionToModel(r)
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func (c pbConverter) RunProgramOptionToPb(r RunProgramOption) (*RunProgramRequest, error) {
	p, err := c.ProgramToPb(r.Program)
	if err != nil {
		return nil, err
	}
	ins, err := c.InstructionersToPb(r.Instructions)
	if err != nil {
		return nil, err
	}
	req := &RunProgramRequest{
		SchedulerId:  r.SchedulerID.String(),
		Program:      p,
		Instructions: ins,
	}
	return req, nil
}

func (c pbConverter) WaitResultOptionToPb(r WaitResultOption) (*WaitResultRequest, error) {
	ids := []string{}
	for _, id := range r.IDs {
		ids = append(ids, id.String())
	}
	return &WaitResultRequest{Ids: ids}, nil
}

func (pbConverter) WaitResultResponseToModel(r *WaitResultResponse) (ProgramResult, error) {
	if r == nil {
		return ProgramResult{}, ErrNilPointer
	}

	id, err := ParseProgramID(r.Id)
	if err != nil {
		return ProgramResult{}, err
	}
	var resultErr error
	if r.Error != "" {
		resultErr = newError(r.Error)
	}
	res := ProgramResult{
		ID:     id,
		Result: r.Result,
		Error:  resultErr,
	}
	return res, nil
}

func (pbConverter) WaitResultResponseFromModel(r ProgramResult) (*WaitResultResponse, error) {
	var error string
	if r.Error != nil {
		error = r.Error.Error()
	}
	result, err := marshal(r.Result)
	if err != nil {
		return nil, err
	}

	res := &WaitResultResponse{
		Id:     r.ID.String(),
		Error:  error,
		Result: result,
	}
	return res, nil
}

func (c pbConverter) ParseExecuteStream(r *ExecuteStream) (interface{}, error) {
	if r == nil {
		return nil, ErrNilPointer
	}

	switch r.Type {
	case ExecuteStream_Connect:
		return c.parseExecuteStreamConnect(r)
	case ExecuteStream_Execute:
		return c.parseExecuteStreamExecute(r)
	case ExecuteStream_Result:
		return c.parseExecuteStreamResult(r)
	default:
		return nil, wrapErrorf(ErrRequest, "Client receive invalid type %s", r.Type)
	}
}

func (pbConverter) parseExecuteStreamConnect(r *ExecuteStream) (interface{}, error) {
	return struct{}{}, nil
}

type executeStreamExecuteRequest struct {
	Op   Instruction
	Args []Instruction
}

func (c pbConverter) parseExecuteStreamExecute(r *ExecuteStream) (interface{}, error) {
	result := executeStreamExecuteRequest{}
	op, err := c.InstructionToModel(r.Op)
	if err != nil {
		return nil, err
	}
	result.Op = op.Instruction()
	for _, v := range r.Args {
		args, err := c.InstructionToModel(v)
		if err != nil {
			return nil, err
		}
		result.Args = append(result.Args, args.Instruction())
	}
	return result, nil
}

func (c pbConverter) parseExecuteStreamResult(r *ExecuteStream) (interface{}, error) {
	return c.InstructionToModel(r.Result)
}

func (c pbConverter) WaitResultRequestToProgramIDs(r *WaitResultRequest) ([]ProgramID, error) {
	if r == nil {
		return nil, ErrNilPointer
	}

	ids := []ProgramID{}
	for _, s := range r.Ids {
		id, err := ParseProgramID(s)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
