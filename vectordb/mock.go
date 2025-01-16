package vectordb

import (
	"github.com/golly-go/golly"
	"github.com/stretchr/testify/mock"
)

type MockVectorDatabase struct {
	mock.Mock
}

func NewMockVectorDatabase() *MockVectorDatabase {
	return &MockVectorDatabase{}
}

func NewTestConnection(ctx *golly.Context) (*golly.Context, *MockVectorDatabase) {
	mck := NewMockVectorDatabase()
	ctx = ConnectionToContext(ctx, mck)
	return ctx, mck
}

func (mvd *MockVectorDatabase) Find(ctx *golly.Context, params FindParams) (VectorRecord, error) {
	args := mvd.Called(ctx, params)
	return args.Get(0).(VectorRecord), args.Error(1)
}

func (mvd *MockVectorDatabase) Search(ctx *golly.Context, params SearchParams) (VectorRecords, error) {
	args := mvd.Called(ctx, params)
	return args.Get(0).(VectorRecords), args.Error(1)
}

func (mvd *MockVectorDatabase) Update(ctx *golly.Context, params UpdateParams) ([]byte, error) {
	args := mvd.Called(ctx, params)
	return args.Get(0).([]byte), args.Error(1)
}
