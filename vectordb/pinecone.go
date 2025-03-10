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
	"github.com/golly-go/plugins/functional"
	"github.com/google/uuid"
)

type Pinecone struct {
	key    string
	Url    *url.URL
	client *http.Client
}

type PineconeConfig struct {
	Key     string
	Project string
	Env     string
	Index   string
}

func (p PineconeConfig) Valid() error {
	if p.Env == "" {
		return fmt.Errorf("vectordb.environment not set")
	}
	if p.Index == "" {
		return fmt.Errorf("vectordb.index not set")
	}
	if p.Project == "" {
		return fmt.Errorf("vectordb.project not set")
	}
	if p.Key == "" {
		return fmt.Errorf("vectordb.key not set")
	}

	return nil
}

func (p PineconeConfig) ConnectionString() (*url.URL, error) {
	return url.Parse(fmt.Sprintf("https://%s-%s.svc.%s.pinecone.io:443", p.Index, p.Project, p.Project))
}

func (p PineconeConfig) APIKey() string {
	return p.Key
}

var _ VectorConfig = PineconeConfig{}

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

func (p *Pinecone) Update(gctx *golly.Context, update UpdateParams) ([]byte, error) {
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

	if golly.Env().IsDevelopment() {
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

func (p *Pinecone) Find(gctx *golly.Context, params FindParams) (VectorRecord, error) {
	results := VectorRecord{}

	if params.ID == uuid.Nil {
		return results, fmt.Errorf("id is required")
	}

	id := params.ID.String()

	url := "/vectors/fetch?ids=" + id

	if params.Namespace != nil && *params.Namespace != "" {
		url += "&namespace=" + *params.Namespace
	}

	req, err := p.request(gctx, url, http.MethodGet, "")
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

func (p *Pinecone) Search(gctx *golly.Context, params SearchParams) (VectorRecords, error) {
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

func (p *Pinecone) request(gctx *golly.Context, path, method, payload string) (*http.Request, error) {
	pieces := strings.Split(path, "?")

	url := p.Url.JoinPath(pieces[0])

	if len(pieces) > 1 {
		url.RawQuery = pieces[1]
	}

	req, err := http.NewRequestWithContext(gctx,
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
func NewPinecone(p VectorConfig) *Pinecone {
	url, err := p.ConnectionString()
	if err != nil {
		panic(err)
	}

	return &Pinecone{
		key:    p.APIKey(),
		Url:    url,
		client: http.DefaultClient,
	}
}

func initializePinecone(app *golly.Application) (*Pinecone, error) {
	config := app.Config()

	cfg := PineconeConfig{
		Key:     config.GetString("vectordb.key"),
		Index:   config.GetString("vectordb.index"),
		Env:     config.GetString("vectordb.environment"),
		Project: config.GetString("vectordb.project"),
	}

	if err := cfg.Valid(); err != nil {
		return nil, err
	}

	return NewPinecone(cfg), nil
}

// -func NewPinecone(config *viper.Viper) *Pinecone {

// 	func NewPinecone(p VectorConfig) *Pinecone {
// 	+       url, err := p.ConnectionString()
// 			if err != nil {
// 					panic(err)
// 			}

// 			return &Pinecone{
// 	-               key:    config.GetString("vectorstore.pinecone.key"),
// 	+               key:    p.APIKey(),
// 					Url:    url,
// 					client: http.DefaultClient,
// 			}
