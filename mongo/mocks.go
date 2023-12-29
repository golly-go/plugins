package mongo

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MockClient struct {
	mock.Mock

	MockCollection *MockCollection
}

func NewMockClient(ctx golly.Context) (golly.Context, *MockClient) {
	client := MockClient{
		MockCollection: &MockCollection{},
	}

	ctx = ctx.Set(mongoClientKey, &client)

	return ctx, &client
}

// Collection provides a mock function with given fields: obj
func (_m *MockClient) Collection(golly.Context, interface{}) Collection {
	return _m.MockCollection
}

// Connect provides a mock function with given fields: ctx
func (_m *MockClient) Connect(ctx golly.Context) error {
	ret := _m.Called(ctx)

	return ret.Error(0)
}

// Disconnect provides a mock function with given fields: ctx
func (_m *MockClient) Disconnect(ctx golly.Context) error {
	ret := _m.Called(ctx)
	return ret.Error(0)
}

func (_m *MockClient) IsConnected(ctx golly.Context) bool {
	_m.Called(ctx)

	return true
}

// Ping provides a mock function with given fields: ctx, timeout
func (_m *MockClient) Ping(ctx golly.Context, timeout ...time.Duration) error {
	ret := _m.Called(ctx, timeout)
	return ret.Error(0)
}

func (_m *MockClient) Transaction(ctx golly.Context, fn func(ctx golly.Context) error) error {
	ret := _m.Called(ctx, fn)
	return ret.Error(0)
}

// Database provides a mock function with given fields: ctx, options
func (_m *MockClient) Database(ctx golly.Context, options DatabaseOptions) Client {
	return _m
}

var _ Client = (*MockClient)(nil)

type MockCollection struct {
	mock.Mock
}

// Find provides a mock function with given fields: out, filter, options
func (_m *MockCollection) Find(out interface{}, filter interface{}, options ...*options.FindOptions) error {
	ret := _m.Called(out, filter, options)

	var r0 error
	if rf, ok := ret.Get(0).(FinderFunc); ok {
		r0 = rf(out, filter, options...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// FindOne provides a mock function with given fields: out, filter
func (_m *MockCollection) FindOne(out interface{}, filter interface{}) error {
	ret := _m.Called(out, filter)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, interface{}) error); ok {
		r0 = rf(out, filter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// FindByID provides a mock function with given fields: out, id
func (_m *MockCollection) FindByID(out interface{}, id interface{}) error {
	ret := _m.Called(out, id)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, interface{}) error); ok {
		r0 = rf(out, id)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Insert provides a mock function with given fields: out
func (_m *MockCollection) Insert(out interface{}) error {
	ret := _m.Called(out)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(out)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateOne provides a mock function with given fields: out, updateDocument
func (_m *MockCollection) UpdateOne(out interface{}, updateDocument interface{}) error {
	ret := _m.Called(out, updateDocument)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, interface{}) error); ok {
		r0 = rf(out, updateDocument)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateOneDocument provides a mock function with given fields: out
func (_m *MockCollection) UpdateOneDocument(out interface{}) error {
	ret := _m.Called(out)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(out)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *MockCollection) C() *mongo.Collection {
	_m.Called()

	return &mongo.Collection{}
}

var _ Collection = (*MockCollection)(nil)
