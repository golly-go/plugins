package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/stretchr/testify/mock"
)

// Repository is a very light wrapper around a datastore
// not all incomposing but read models should be implemented outside of this
type Repository interface {
	Load(ctx golly.Context, object interface{}) error
	Save(ctx golly.Context, object interface{}) error

	Transaction(golly.Context, func(golly.Context, Repository) error) error
	IsNewRecord(obj interface{}) bool
}

type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) Load(ctx golly.Context, object interface{}) error {
	args := m.Called(ctx, object)
	return args.Error(0)
}

func (m *MockRepository) Save(ctx golly.Context, object interface{}) error {
	args := m.Called(ctx, object)

	return args.Error(0)
}

func (m *MockRepository) Transaction(ctx golly.Context, fn func(golly.Context, Repository) error) error {
	args := m.Called(ctx, fn)
	return args.Error(0)
}

func (m *MockRepository) IsNewRecord(obj interface{}) bool {
	args := m.Called(obj)
	return args.Bool(0)
}

func NewTestRepository(ctx golly.Context) (golly.Context, Repository) {
	repo := &MockRepository{}

	ctx = ctx.Set(MockRepository{}, repo)

	return ctx, repo
}

func repoFromContext(ctx golly.Context) Repository {
	val, _ := ctx.Get(MockRepository{})

	if val == nil {
		return nil
	}

	return val.(Repository)
}
