package vectordb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/env"
	"github.com/golly-go/plugins/functional"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

type Pinecone struct {
	key    string
	Url    *url.URL
	client *http.Client
}

type Owner struct {
	ID   uuid.UUID `json:"ownerID"`
	Type string    `json:"ownerType"`
}

type Metadata struct {
	Owner
}

type SearchPineconeResponse struct {
	Matches []VectorRecord   `json:"matches"`
	Results []map[string]any `json:"results"`
}

type PineconeFetchResponse struct {
	Vectors map[string]VectorRecord `json:"vectors"`
}

func (p *Pinecone) Update(gctx golly.Context, update UpdateParams) ([]byte, error) {
	defer func(start time.Time) {
		gctx.Logger().WithField("duration", time.Since(start)).Debugf("Upserted (%d) objects", len(update.Records))

	}(time.Now())

	if len(update.Records) == 0 {
		return []byte{}, fmt.Errorf("nothing to update")
	}

	b, _ := json.MarshalIndent(update, "", "\t")

	req, err := p.request(gctx, "/vectors/upsert", http.MethodPost, string(b))
	if err != nil {
		return []byte{}, err
	}

	if env.IsDevelopment() {
		gctx.Logger().Debugf("Upserted: (%s)", strings.Join(functional.MapStrings[VectorRecord](update.Records, func(up VectorRecord) string {
			return fmt.Sprintf("%s", up.ID.String())
		}), ","))
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return []byte{}, err
	}

	defer res.Body.Close()

	return io.ReadAll(res.Body)
}

func (p *Pinecone) Find(gctx golly.Context, id string) (VectorRecord, error) {
	results := VectorRecord{}

	req, err := p.request(gctx, "/vectors/fetch?ids="+id, http.MethodGet, "")
	if err != nil {
		return results, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return results, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return results, err
	}

	pineconeGetResult := PineconeFetchResponse{}

	if err := json.Unmarshal(body, &pineconeGetResult); err != nil {
		return results, err
	}

	return pineconeGetResult.Vectors[id], nil
}

func (p *Pinecone) Search(gctx golly.Context, params SearchParams) (VectorRecords, error) {
	results := []VectorRecord{}

	params.IncludeMetadata = true

	if params.TopK == 0 {
		params.TopK = 10
	}

	b, _ := json.MarshalIndent(params, "", "\t")

	req, err := p.request(gctx, "/query", http.MethodPost, string(b))
	if err != nil {
		return results, nil
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return results, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return results, err
	}

	searchResponse := SearchPineconeResponse{}

	if err := json.Unmarshal(body, &searchResponse); err != nil {
		return results, err
	}

	return searchResponse.Matches, nil
}

func (p *Pinecone) request(gctx golly.Context, path, method, payload string) (*http.Request, error) {
	pieces := strings.Split(path, "?")

	url := p.Url.JoinPath(pieces[0])

	if len(pieces) > 1 {
		url.RawQuery = pieces[1]
	}

	req, err := http.NewRequestWithContext(gctx.Context(),
		method,
		url.String(),
		strings.NewReader(payload),
	)

	if err != nil {
		return nil, err
	}

	req.Header.Add("Api-Key", p.key)
	req.Header.Add("content-type", "application/json")
	req.Header.Add("accept", "application/json")

	return req, nil
}

// NewPinecone creates a new Pinecone client.
func NewPinecone(config *viper.Viper) *Pinecone {
	url, err := url.Parse(fmt.Sprintf("https://%s-%s.svc.%s.pinecone.io:443",
		config.GetString("vectorstore.pinecone.index"),
		config.GetString("vectorstore.pinecone.project"),
		config.GetString("vectorstore.pinecone.environment")))

	if err != nil {
		panic(err)
	}

	return &Pinecone{
		key:    config.GetString("vectorstore.pinecone.key"),
		Url:    url,
		client: http.DefaultClient,
	}
}
