package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

// OAuthBearerAuth implements sasl.Mechanism for OAUTHBEARER authentication
// with a dynamic token provider function.
//
// This wraps franz-go's oauth.Auth to call the TokenProvider on each
// authentication attempt, ensuring fresh tokens for services like AWS/GCP
// where tokens expire.
type OAuthBearerAuth struct {
	// TokenProvider is called each time authentication is needed.
	// It should return a valid OAuth token string and any error.
	TokenProvider func() (string, error)
}

// Name returns the SASL mechanism name.
func (o OAuthBearerAuth) Name() string {
	return "OAUTHBEARER"
}

// Authenticate initializes an authentication session.
// Calls TokenProvider to get a fresh token and delegates to franz-go's oauth.Auth.
func (o OAuthBearerAuth) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	token, err := o.TokenProvider()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get OAuth token: %w", err)
	}

	// Use franz-go's oauth.Auth which properly formats the OAUTHBEARER message
	auth := oauth.Auth{
		Token: token,
	}

	return auth.AsMechanism().Authenticate(ctx, host)
}
