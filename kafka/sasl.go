package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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

	// PLAIN is a single-step mechanism, return a completed session
	return &plainSession{}, msg, nil
}

// plainSession implements sasl.Session for PLAIN (single-step auth)
type plainSession struct{}

// Challenge handles server response. PLAIN is single-step, so if we get here
// with data, authentication failed.
func (s *plainSession) Challenge(resp []byte) (bool, []byte, error) {
	// PLAIN is single-step - if server sends anything, it's an error
	if len(resp) > 0 {
		return false, nil, fmt.Errorf("PLAIN authentication failed: %s", string(resp))
	}
	// Empty response means success
	return true, nil, nil
}

func saslMechanism(config Config) (kgo.Opt, error) {
	switch config.SASL {
	case SASLOAUTHCustom:
		if config.CustomSASLMechanism == nil {
			return nil, ErrTokenProviderRequired
		}
		return kgo.SASL(oauth.Oauth(config.CustomSASLMechanism)), nil

	case SASLOAUTHPlain:
		// PLAIN mechanism with OAuth token as password
		// Used by GCP Managed Kafka and some other managed services
		if config.TokenProvider == nil {
			return nil, ErrTokenProviderRequired
		}
		return kgo.SASL(OAuthPlainAuth{
			User:          config.Username, // Can be empty
			TokenProvider: config.TokenProvider,
		}), nil

	case SASLPlain:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequired
		}
		return kgo.SASL(plain.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsMechanism()), nil

	case SASLScramSHA256:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequiredSCRAMSHA256
		}

		return kgo.SASL(scram.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsSha256Mechanism()), nil

	case SASLScramSHA512:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequiredSCRAMSHA512
		}

		return kgo.SASL(scram.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsSha512Mechanism()), nil
	}

	return nil, ErrInvalidSASLMechanism
}
