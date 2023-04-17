package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestDocument struct {
	Document
}

type TestDocumentWithName struct {
	Document
}

func (TestDocumentWithName) CollectionName() string { return "brah" }

func BenchmarkCollectingNameing(b *testing.B) {
	b.Run("collection#notnamer", func(b *testing.B) {
		collectionName(&TestDocument{})
	})

	b.Run("collection#namer", func(b *testing.B) {
		collectionName(&TestDocumentWithName{})
	})
}

func TestCollectionNameing(t *testing.T) {
	t.Run("it should name correct when collectionname func is not defined", func(t *testing.T) {
		examples := []interface{}{
			&TestDocument{},
			TestDocument{},
			[]TestDocument{},
		}

		for _, example := range examples {
			s, err := collectionName(example)
			assert.NoError(t, err)

			assert.Equal(t, "test_documents", s)
		}
	})

	t.Run("it should name correct when CollectionName func is defined", func(t *testing.T) {
		examples := []interface{}{
			&TestDocumentWithName{},
			TestDocumentWithName{},
			[]TestDocumentWithName{},
		}

		for _, example := range examples {
			s, err := collectionName(example)
			assert.NoError(t, err)
			assert.NotEqual(t, s, "test_document_with_name")
			assert.Equal(t, "brah", s)
		}
	})

	t.Run("it throw an error if type is not supported", func(t *testing.T) {
		_, err := collectionName(1)
		assert.Error(t, err)
	})

}
