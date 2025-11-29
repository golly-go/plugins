package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
)

// OAuthPlainAuth implements SASL PLAIN where the password is an OAuth token.
// This is used by some managed Kafka services (like GCP Managed Kafka) that
// expect the OAuth token to be passed via PLAIN mechanism.
type OAuthPlainAuth struct {
	// User is the SASL username (often empty or a service account identifier)
	User string

	// TokenProvider is called each time authentication is needed.
	// It should return a valid OAuth token string to use as the password.
	TokenProvider func() (string, error)
}

// Name returns the SASL mechanism name.
func (o OAuthPlainAuth) Name() string {
	return "PLAIN"
}

// Authenticate initializes an authentication session.
func (o OAuthPlainAuth) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	token, err := o.TokenProvider()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get OAuth token: %w", err)
	}

	// PLAIN format: \x00<username>\x00<password>
	// For OAuth, password is the access token
	msg := []byte(fmt.Sprintf("\x00%s\x00%s", o.User, token))

	return nil, msg, nil
}
