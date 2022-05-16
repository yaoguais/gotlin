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
}

func newServerService(g *Gotlin) *serverService {
	return &serverService{g: g}
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
	ctx := stream.Context()
	p, _ := peer.FromContext(ctx)
	host := Host(p.Addr.String())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		r, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		println("server receive ==> ", r.String())

		if r.Type == ExecuteStream_Connect {
			commander := NewCommander(s.g.executorPool, host, stream)
			err := s.g.executorPool.attachCommander(commander)
			if err != nil {
				return err
			}
		} else if r.Type == ExecuteStream_Result {
			id, _ := ParseID(r.Id)
			err := s.g.executorPool.setRemoteExecuteResult(id, r.Result.Result)
			if err != nil {
				return err
			}
		}
	}
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
