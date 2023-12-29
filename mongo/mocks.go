package mongo

import (
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MockClient struct {
	mock *mock.Mock

	MockCollection *MockCollection
}

func NewMockClient() MockClient {
	return MockClient{
		mock:           &mock.Mock{},
		MockCollection: &MockCollection{},
	}
}

// Collection provides a mock function with given fields: obj
func (_m *MockClient) Collection(obj interface{}) Collection {
	return _m.MockCollection
}

type MockCollection struct {
	mock *mock.Mock
}

// Find provides a mock function with given fields: out, filter, options
func (_m *MockCollection) Find(out interface{}, filter interface{}, options ...*options.FindOptions) error {
	ret := _m.mock.Called(out, filter, options)

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
	ret := _m.mock.Called(out, filter)

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
	ret := _m.mock.Called(out, id)

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
	ret := _m.mock.Called(out)

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
	ret := _m.mock.Called(out, updateDocument)

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
	ret := _m.mock.Called(out)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(out)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *MockCollection) C() *mongo.Collection {
	_m.mock.Called()

	return &mongo.Collection{}
}

var _ Collection = (*MockCollection)(nil)
