package gotlin

import (
	"context"
	"io"

	"google.golang.org/grpc/peer"
)

var _ ServerServiceServer = (*serverService)(nil)

type serverService struct {
	UnimplementedServerServiceServer

	g *Gotlin
	l serverLogger
	formatError
}

func newServerService(g *Gotlin) *serverService {
	fe := formatError{"Server " + g.id + " %s"}
	return &serverService{g: g, formatError: fe, l: g.l}
}

func (s *serverService) RegisterExecutor(ctx context.Context, req *RegisterExecutorRequest) (resp *RegisterExecutorResponse, err error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, newErrorf("Unrecognized client address")
	}
	req.Host = p.Addr.String()
	executor, err := newExecutorFromClient(req)
	if err != nil {
		return
	}
	err = s.g.executorPool.Add(ctx, executor)
	if err != nil {
		return
	}
	return &RegisterExecutorResponse{}, nil
}

func (s *serverService) UnregisterExecutor(ctx context.Context, req *UnregisterExecutorRequest) (resp *UnregisterExecutorResponse, err error) {
	id, err := ParseExecutorID(req.Id)
	if err != nil {
		return
	}
	var removeErr error
	if req.Error != "" {
		removeErr = newError(req.Error)
	}

	err = s.g.executorPool.Remove(ctx, id, removeErr)
	if err != nil {
		return
	}
	return &UnregisterExecutorResponse{}, nil
}

func (s *serverService) Execute(stream ServerService_ExecuteServer) error {
	return s.executeLoop(stream)
}

func (s *serverService) executeLoop(stream ServerService_ExecuteServer) error {
	r, err := stream.Recv()
	if err != nil {
		return s.error(ErrResponse, err, "Receive connection requests from compute nodes")
	}

	if r.Type != ExecuteStream_Connect {
		return s.error(ErrResponse, ErrUndoubted, "The first request of the compute node is not a connection request")
	}

	l := s.l.WithExecuteID(r.Id)
	logger := l.Logger()
	logger.Debugf("Receive a connection instruction, %s", r)

	ctx := stream.Context()
	p, _ := peer.FromContext(ctx)
	host := Host(p.Addr.String())

	executorID, err := s.g.executorPool.FindByHost(ctx, host)
	if err != nil {
		return s.error(ErrResponse, ErrUndoubted, "The compute node is not registered, "+string(host))
	}

	l = s.l.WithExecutor(executorID.String(), host)
	logger = l.Logger()

	executor := newExecutor(s.g.executorPool, host, stream, l)
	err = s.g.executorPool.attachExecutor(executor)
	if err != nil {
		return s.error(ErrResponse, err, "Add compute nodes to the compute pool")
	}

	logger.Info("A new compute node is successfully connected")
	defer logger.Info("The compute node has been disconnected")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := s.execute(stream, l)
		if err != nil {
			return err
		}
	}
}

func (s *serverService) execute(stream ServerService_ExecuteServer, l serverLogger) error {
	r, err := stream.Recv()
	if err == io.EOF {
		return nil
	} else if err != nil {
		return s.error(ErrReceive, err, "Read instruction execution results from computing nodes")
	}

	if r.Type != ExecuteStream_Result {
		return s.error(ErrResponse, ErrUndoubted, "Server receive invalid type "+r.Type.String())
	}

	l = l.WithExecuteID(r.Id)
	logger := l.Logger()

	logger.Debugf("Received the result of an instruction, %s", r)

	id, err := ParseExecuteID(r.Id)
	if err != nil {
		return s.error(ErrResponse, err, "Parse execute ID")
	}

	err = s.g.executorPool.setRemoteExecuteResult(id, r.Result.Result)
	return s.error(ErrResponse, err, "Save the execution result of the instruction")
}

func (s *serverService) RequestScheduler(ctx context.Context, r *RequestSchedulerRequest) (*RequestSchedulerResponse, error) {
	option := pbConverter{}.RequestSchedulerRequestToModel(r)
	id, err := s.g.RequestScheduler(ctx, option)
	if err != nil {
		return nil, err
	}
	return &RequestSchedulerResponse{
		Id: id.String(),
	}, nil
}

func (s *serverService) RunProgram(ctx context.Context, r *RunProgramRequest) (*RunProgramResponse, error) {
	pc := pbConverter{}
	id, _ := ParseSchedulerID(r.SchedulerId)
	p := pc.ProgramToModel(r.Program)
	ins := pc.InstructionToModels(r.Instructions)

	err := s.g.RunProgram(context.Background(), id, p, ins)
	if err != nil {
		return nil, err
	}
	return &RunProgramResponse{}, nil
}

func (s *serverService) WaitResult(r *WaitResultRequest, stream ServerService_WaitResultServer) error {
	ctx := stream.Context()
	ch, err := s.g.WaitResult(ctx)
	if err != nil {
		return err
	}

	pc := pbConverter{}
	for result := range ch {
		err := stream.Send(pc.WaitResultResponseFromModel(result))
		if err != nil {
			return err
		}
	}

	return nil
}
