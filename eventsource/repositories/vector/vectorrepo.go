package vector

import (
	"encoding/json"
	"fmt"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/golly-go/golly/utils"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/vectordb"
	"github.com/google/uuid"
)

type VecorRepo struct{}

func (VecorRepo) Load(gctx golly.Context, object interface{}) error {
	connection := vectordb.Connection(gctx)

	id, ok := utils.IDField(object).(uuid.UUID)
	if !ok {
		return fmt.Errorf("ID field is not a uuid: %v", id)
	}

	c, _ := helpers.CollectionName(object)

	record, err := connection.Find(gctx, vectordb.FindParams{
		ID:        id,
		Namespace: &c,
	})

	if err != nil {
		return errors.WrapGeneric(err)
	}

	if record.ID == uuid.Nil {
		return errors.WrapNotFound(fmt.Errorf("record not found"))
	}

	if b, err := json.Marshal(record); err == nil {
		if err := json.Unmarshal(b, object); err != nil {
			return errors.WrapGeneric(err)
		}
	} else {
		return errors.WrapGeneric(err)
	}

	return nil
}

func (r VecorRepo) Save(gctx golly.Context, object interface{}) error {
	vm, ok := object.(vectordb.VectorModel)
	if !ok {
		return fmt.Errorf("object is not a vector model: %v", object)
	}

	c, _ := helpers.CollectionName(object)

	_, err := vectordb.
		Connection(gctx).
		Update(gctx, vectordb.UpdateParams{
			Namespace: &c,
			Records:   vectordb.VectorRecords{vm.VectorRecord(gctx)},
		})

	return err
}

func (VecorRepo) IsNewRecord(obj interface{}) bool {
	return true
}

func (r VecorRepo) Transaction(ctx golly.Context, fn func(golly.Context, eventsource.Repository) error) error {
	return fn(ctx, r)
}

var _ = eventsource.Repository(VecorRepo{})
