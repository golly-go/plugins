package vectordb

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/env"
	"github.com/golly-go/plugins/functional"
	"github.com/google/uuid"
)

var (
	Driver VectorDatabase

	vectorContext golly.ContextKeyT = "vectorDBConnection"
)

type VectorDatabase interface {
	Find(golly.Context, string) (VectorRecord, error)
	Search(golly.Context, SearchParams) (VectorRecords, error)
	Update(golly.Context, UpdateParams) ([]byte, error)
}

type UpdateParams struct {
	Records   VectorRecords `json:"vectors"`
	Namespace *string       `json:"namespace,omitempty"`
}

type SearchParams struct {
	Namespace       *string   `json:"namespace,omitempty"`
	Filter          any       `json:"filter,omitempty"`
	IncludeValues   bool      `json:"includeValues"`
	TopK            int       `json:"topK"`
	Vector          []float64 `json:"vector"`
	IncludeMetadata bool      `json:"includeMetadata"`
	CutOff          float64   `json:"-"`
}

type EmbeddingText interface {
	GenerateVector(golly.Context) ([]float64, error)
}

type VectorModel interface {
	VectorRecord(ctx golly.Context) VectorRecord
}

type VectorRecord struct {
	EmbeddingText `json:"-"`

	ID uuid.UUID `json:"id"`

	Vectors []float64 `json:"values"`
	Score   *float64  `json:"score,omitempty"`

	Metadata interface{} `json:"metadata"`
}

func (vr *VectorRecord) GenerateVector(ctx golly.Context) (err error) {
	vr.Vectors, err = vr.EmbeddingText.GenerateVector(ctx)
	return
}

func (vr *VectorRecord) Valid() bool {
	return vr.ID != uuid.Nil && len(vr.Vectors) > 0
}

type VectorRecords []VectorRecord

func (vrs VectorRecords) GenerateVectors(ctx golly.Context) VectorRecords {
	ret := functional.AsyncMap[VectorRecord, VectorRecord](vrs, func(entry VectorRecord) VectorRecord {
		if err := entry.GenerateVector(ctx); err != nil {
			ctx.Logger().Warnf("GenerateVectors: %v", err)
			return VectorRecord{}
		}
		return entry
	})

	return VectorRecords(ret)
}

func (vrs VectorRecords) Valid() VectorRecords {
	return VectorRecords(functional.Filter[VectorRecord](vrs, func(entry VectorRecord) bool {
		return entry.Valid()
	}))
}

func UpsertVectorRecordObjects(gctx golly.Context, namespace string, objects ...VectorModel) ([]byte, error) {
	return Upsert(gctx, UpdateParams{
		Namespace: &namespace,
		Records: functional.Map[VectorModel, VectorRecord](objects, func(v VectorModel) VectorRecord {
			return v.VectorRecord(gctx)
		}),
	})
}

func Upsert(gctx golly.Context, update UpdateParams) ([]byte, error) {
	if len(update.Records) == 0 {
		return []byte{}, nil
	}

	if update.Namespace != nil && *update.Namespace == "" {
		update.Namespace = nil
	}

	update.Records = update.
		Records.
		GenerateVectors(gctx).
		Valid()

	return Connection(gctx).Update(gctx, update)
}

func Find(gctx golly.Context, id string) (VectorRecord, error) {
	return Connection(gctx).Find(gctx, id)
}

func Search(gctx golly.Context, search SearchParams) (VectorRecords, error) {

	if search.Namespace != nil && *search.Namespace == "" {
		search.Namespace = nil
	}

	results, err := Connection(gctx).Search(gctx, search)
	if err != nil {
		return results, err
	}

	if search.CutOff == 0 {
		return results, nil
	}

	return functional.Filter[VectorRecord](results, func(res VectorRecord) bool {
		return res.Score != nil && *res.Score >= search.CutOff
	}), nil
}

func ConnectionToContext(ctx golly.Context, db VectorDatabase) golly.Context {
	return ctx.Set(vectorContext, db)
}

func Connection(ctx golly.Context) VectorDatabase {
	if driver, ok := ctx.Get(vectorContext); ok {
		if d, ok := driver.(VectorDatabase); ok {
			return d
		}
	}

	// Guard here to prevent to prevent this ever
	// running env.IsTest() for now, we may want an integraiton layer
	// but we should accurately set the ENV for it
	if env.IsTest() {
		return NewMockVectorDatabase()
	}

	return Driver
}

func initializeDefaultConfig(app golly.Application) {
	app.Config.SetDefault("vectorstore", map[string]any{
		"pinecone": map[string]any{
			"key":         "8d94df3e-ecdd-4995-a914-302e4af54fbf",
			"project":     "2b75b17",
			"environment": "us-west4-gcp-free",
			"index":       "dev",
		},
	})
}

func Initializer(app golly.Application) error {
	initializeDefaultConfig(app)

	Driver = NewPinecone(app.Config)

	return nil
}
