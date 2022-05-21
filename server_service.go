package gotlin

import (
	"context"

	. "github.com/yaoguais/gotlin/proto" //revive:disable-line
	"google.golang.org/grpc/peer"
)

var _ ServerServiceServer = (*serverServiceImpl)(nil)

type serverServiceImpl struct {
	UnimplementedServerServiceServer

	g *Gotlin
	l serverLogger
	formatError
}

func newServerService(g *Gotlin) *serverServiceImpl {
	fe := formatError{"Server " + g.id + " %s"}
	return &serverServiceImpl{g: g, formatError: fe, l: g.l}
}

func (s *serverServiceImpl) RegisterExecutor(ctx context.Context, req *RegisterExecutorRequest) (resp *RegisterExecutorResponse, err error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, newErrorf("Unrecognized client address")
	}
	req.Host = p.Addr.String()
	executor, err := pbConverter{}.RegisterExecutorRequestToModel(req)
	if err != nil {
		return
	}
	err = s.g.executorPool.Add(ctx, executor)
	if err != nil {
		return
	}
	metrics.AddExecutor()
	return &RegisterExecutorResponse{}, nil
}

func (s *serverServiceImpl) UnregisterExecutor(ctx context.Context, req *UnregisterExecutorRequest) (resp *UnregisterExecutorResponse, err error) {
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
	metrics.RemoveExecutor()
	return &UnregisterExecutorResponse{}, nil
}

func (s *serverServiceImpl) Execute(stream ServerService_ExecuteServer) error {
	return s.executeLoop(stream)
}

func (s *serverServiceImpl) executeLoop(stream ServerService_ExecuteServer) error {
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

	executor := newExecutor(s.g.executorPool, host, stream, l, s.formatError)
	err = s.g.executorPool.Attach(executor)
	if err != nil {
		return s.error(ErrResponse, err, "Add compute nodes to the compute pool")
	}
	defer s.g.executorPool.Detach(executor)

	logger.Info("A new compute node is successfully connected")
	defer logger.Info("The compute node has been disconnected")

	<-ctx.Done()

	return nil
}

func (s *serverServiceImpl) RequestScheduler(ctx context.Context, r *RequestSchedulerRequest) (*RequestSchedulerResponse, error) {
	option, err := pbConverter{}.RequestSchedulerRequestToModel(r)
	if err != nil {
		return nil, s.error(ErrConverter, err, "RequestSchedulerRequest")
	}
	id, err := s.g.RequestScheduler(ctx, option)
	if err != nil {
		return nil, err
	}
	return &RequestSchedulerResponse{
		Id: id.String(),
	}, nil
}

func (s *serverServiceImpl) RunProgram(ctx context.Context, r *RunProgramRequest) (*RunProgramResponse, error) {
	pc := pbConverter{}
	id, err := ParseSchedulerID(r.SchedulerId)
	if err != nil {
		return nil, s.error(ErrConverter, err, "RunProgramRequest")
	}
	p, err := pc.ProgramToModel(r.Program)
	if err != nil {
		return nil, s.error(ErrConverter, err, "RunProgramRequest")
	}
	ins, err := pc.InstructionToModels(r.Instructions)
	if err != nil {
		return nil, s.error(ErrConverter, err, "RunProgramRequest")
	}

	err = s.g.RunProgram(context.Background(), id, p, ins)
	if err != nil {
		return nil, err
	}
	return &RunProgramResponse{}, nil
}

func (s *serverServiceImpl) WaitResult(r *WaitResultRequest, stream ServerService_WaitResultServer) error {
	ctx := stream.Context()
	pc := pbConverter{}

	ids, err := pc.WaitResultRequestToProgramIDs(r)
	if err != nil {
		return err
	}
	ch, err := s.g.WaitResult(ctx, ids)
	if err != nil {
		return err
	}

	for result := range ch {
		pb, err := pc.WaitResultResponseFromModel(result)
		if err != nil {
			return err
		}
		err = stream.Send(pb)
		if err != nil {
			return err
		}
	}

	return nil
}
