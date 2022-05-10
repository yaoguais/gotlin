package gotlin

import (
	"context"
	"io"

	"github.com/pkg/errors"
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
		return nil, errors.Errorf("Unrecognized client address")
	}
	executor, err := newExecutorFromClient(req)
	executor.Host = Host(p.Addr.String()) // TODO fix it
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
		removeErr = errors.New(req.Error)
	}

	err = s.g.executorPool.Remove(ctx, id, removeErr)
	if err != nil {
		return
	}
	return &UnregisterExecutorResponse{}, nil
}

func (s *serverService) ExecuteCommand(stream ServerService_ExecuteCommandServer) error {
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

		if r.Type == CommandType_ConnectToServer {
			commander := NewCommander(s.g.executorPool, host, stream)
			err := s.g.executorPool.attachCommander(commander)
			if err != nil {
				return err
			}
		} else if r.Type == CommandType_ExecuteInstruction {
			id, _ := ParseID(r.Id)
			err := s.g.executorPool.setRemoteExecuteResult(id, r.ExecuteInstruction.Result)
			if err != nil {
				return err
			}
		}
	}
}

func (s *serverService) RequestScheduler(context.Context, *RequestSchedulerRequest) (*RequestSchedulerResponse, error) {
	return &RequestSchedulerResponse{}, nil
}

func (s *serverService) RunProgram(context.Context, *RunProgramRequest) (*RunProgramResponse, error) {
	return &RunProgramResponse{}, nil
}

func (s *serverService) WaitResult(*WaitResultRequest, ServerService_WaitResultServer) error {
	return nil
}
