package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
)

// OAuthBearerAuth implements sasl.Mechanism for OAUTHBEARER authentication
// with a dynamic token provider function.
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
func (o OAuthBearerAuth) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	token, err := o.TokenProvider()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get OAuth token: %w", err)
	}

	// Build the client-first message for OAUTHBEARER
	// Format: "n,,\x01auth=Bearer <token>\x01\x01"
	msg := fmt.Sprintf("n,,\x01auth=Bearer %s\x01\x01", token)

	return &oauthSession{}, []byte(msg), nil
}

// oauthSession implements sasl.Session for OAUTHBEARER
type oauthSession struct{}

// Challenge handles server challenges. For OAUTHBEARER, if we get here,
// authentication failed and the server is sending an error.
func (s *oauthSession) Challenge(resp []byte) (bool, []byte, error) {
	// If the server sends a challenge, it means authentication failed.
	// The response contains the error message from the server.
	if len(resp) > 0 {
		return false, nil, fmt.Errorf("OAUTHBEARER authentication failed: %s", string(resp))
	}
	// Empty response means success
	return true, nil, nil
}
