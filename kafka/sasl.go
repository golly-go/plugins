package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func saslMechanism(config Config) (kgo.Opt, error) {
	if config.SASLMechanism != nil {
		return kgo.SASL(config.SASLMechanism), nil
	}

	switch config.SASL {

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
