package gql

import (
	"context"

	"github.com/golly-go/golly"
)

type ConfigOption func(*Config)

type Config struct {
	identityFunc func(ctx context.Context) golly.Identity
}

func WithIdentityFunc(identityFunc func(ctx context.Context) golly.Identity) ConfigOption {
	return func(o *Config) {
		o.identityFunc = identityFunc
	}
}
