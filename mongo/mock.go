package mongo

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/mock"
)

const (
	mongoContextKey golly.ContextKeyT = "mongoDBDriver"
)

type MockMongoDB struct {
	mock.Mock
}

func (m *MockMongoDB) Collection(ctx golly.Context, obj interface{}) Collection {
	args := m.Called(ctx, obj)
	return args.Get(0).(Collection)
}

func (m *MockMongoDB) Connect(ctx golly.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMongoDB) Disconnect(ctx golly.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMongoDB) IsConnected(ctx golly.Context) bool {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *MockMongoDB) Database(ctx golly.Context, options DatabaseOptions) Client {
	args := m.Called(ctx, options)
	return args.Get(0).(Client)
}

func (m *MockMongoDB) Transaction(ctx golly.Context, fn func(ctx golly.Context) error) error {
	args := m.Called(ctx, fn)
	return args.Error(0)
}

func (m *MockMongoDB) Ping(ctx golly.Context, timeout ...time.Duration) error {
	args := m.Called(ctx, timeout)
	return args.Error(0)
}

func NewTestConnection(ctx golly.Context) (golly.Context, *MockMongoDB) {
	repo := &MockMongoDB{}

	ctx = ctx.Set(mongoContextKey, repo)

	return ctx, repo
}

func connectionFromContext(ctx golly.Context) *MockMongoDB {
	val, _ := ctx.Get(mongoContextKey)

	if val == nil {
		return nil
	}

	return val.(*MockMongoDB)
}
