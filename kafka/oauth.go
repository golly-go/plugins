package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2/google"
)

// GCPTokenProvider implements oauth.TokenProvider for GCP Managed Kafka.
// It fetches and caches OAuth tokens using Google's default credentials,
// refreshing them automatically when they expire.
type GCPTokenProvider struct {
	token  string
	expiry time.Time
	buffer time.Duration // Time before expiry to refresh
	mu     sync.RWMutex
}

// NewGCPTokenProvider creates a token provider with a 5-minute refresh buffer.
func NewGCPTokenProvider() *GCPTokenProvider {
	return &GCPTokenProvider{
		buffer: 5 * time.Minute,
	}
}

// Authorize implements oauth.TokenProvider interface.
// It returns a valid OAuth token, refreshing if necessary.
func (p *GCPTokenProvider) Authorize(ctx context.Context) (oauth.Auth, error) {
	token, err := p.getToken(ctx)
	if err != nil {
		return oauth.Auth{}, err
	}

	return oauth.Auth{
		Token: token,
		// Zid: "",        // Optional: only for impersonation
		// Extensions: {}, // Optional: extra metadata
	}, nil
}

// getToken returns a cached token or fetches a fresh one if expired.
func (p *GCPTokenProvider) getToken(ctx context.Context) (string, error) {
	// Fast path: check if cached token is still valid
	p.mu.RLock()
	if p.token != "" && time.Now().Before(p.expiry.Add(-p.buffer)) {
		token := p.token
		p.mu.RUnlock()
		return token, nil
	}
	p.mu.RUnlock()

	// Slow path: fetch new token
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if p.token != "" && time.Now().Before(p.expiry.Add(-p.buffer)) {
		return p.token, nil
	}

	// Fetch new token from GCP
	ts, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return "", err
	}

	tok, err := ts.Token()
	if err != nil {
		return "", err
	}

	// Cache the token
	p.token = tok.AccessToken
	p.expiry = tok.Expiry

	return p.token, nil
}

// NewGCPOAuthMechanism creates a SASL mechanism for GCP Managed Kafka OAuth authentication.
// This uses franz-go's built-in OAUTHBEARER implementation with GCP token fetching.
func NewGCPOAuthMechanism() sasl.Mechanism {
	return oauth.Oauth(NewGCPTokenProvider().Authorize)
}
