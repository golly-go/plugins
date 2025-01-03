package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/stretchr/testify/mock"
)

type MockCommandHandler struct {
	mock.Mock
}

func (m *MockCommandHandler) Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	args := m.Called(ctx, ag, cmd, metadata)
	return args.Error(0)
}

func (m *MockCommandHandler) Execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	args := m.Called(ctx, ag, cmd, metadata)
	return args.Error(0)
}

func UseMockHandler(gctx golly.Context, mockHandler *MockCommandHandler) golly.Context {
	return gctx.Set(commandHandlerKey, mockHandler)
}

var _ CommandHandler = &MockCommandHandler{}
