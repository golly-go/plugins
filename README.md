# Golly Plugins

This repository contains the official plugin collection for the [Golly](https://github.com/golly-go/golly) framework.

These plugins are designed to be modular and lightweight, allowing you to opt-in to specific functionality without bloating your core application or dependencies.

## Plugins

| Plugin          | Import Path                               | Description                                                                              |
| --------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------- |
| **ORM**         | `github.com/golly-go/plugins/orm`         | Persistence layer using [GORM](https://gorm.io) with support for PostgreSQL and SQLite.  |
| **Eventsource** | `github.com/golly-go/plugins/eventsource` | A lightweight Event Sourcing engine for building event-driven systems.                   |
| **Kafka**       | `github.com/golly-go/plugins/kafka`       | Producer and Consumer implementation using [Franz-go](https://github.com/twmb/franz-go). |
| **Redis**       | `github.com/golly-go/plugins/redis`       | Redis client integration for caching and key-value storage.                              |
| **GraphQL**     | `github.com/golly-go/plugins/gql`         | GraphQL integration for Golly applications.                                              |

## Installation

Since Golly plugins are managed as separate Go modules (using a Go workspace locally), you should install only what you need:

```bash
# Install ORM
go get github.com/golly-go/plugins/orm

# Install Kafka
go get github.com/golly-go/plugins/kafka
```

## Usage

### ORM Plugin

The ORM plugin provides a configured GORM instance attached to your Golly Application.

```go
package main

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/orm"
)

func main() {
	app := golly.New()

	// Initialize ORM
	app.RegisterModule(orm.Module())

    app.Run()
}
```

### Eventsource Plugin

The Eventsource plugin allows you to define aggregates and event handlers.

```go
package main

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

func main() {
    app := golly.New()

    // Register the Eventsource engine
    app.RegisterModule(eventsource.Module())

    app.Run()
}
```

## Contributing

Contributions are welcome! Please ensure any new plugins follow the existing pattern of being self-contained modules.

## License

MIT
