package gql

import (
	"context"

	"github.com/golly-go/golly"
)

type ConfigOption func(*Config)

type Config struct {
	identityFunc func(ctx context.Context) golly.Identity
	schemaOpts   []SchemaOption
}

func WithIdentityFunc(identityFunc func(ctx context.Context) golly.Identity) ConfigOption {
	return func(o *Config) {
		o.identityFunc = identityFunc
	}
}

func WithSchemaOptions(opts ...SchemaOption) ConfigOption {
	return func(o *Config) {
		o.schemaOpts = append(o.schemaOpts, opts...)
	}
}

// RegisterIdentityFunc sets the package-level identity resolver used by GraphQL handlers.
func RegisterIdentityFunc(fn func(ctx context.Context) golly.Identity) {
	identityFunc = fn
}
