# Kafka Golly Plugin/Service

WIP

## Consumers

### Config
Default Configuration:

```
	config.SetDefault("kafka.consumer", map[string]interface{}{
		"bytes": map[string]int{
			"min": 10e3,
			"max": 10e6,
		},
		"group_id":  "1",
		"partition": 0,
		"wait":      "50ms",
		"brokers":   []string{"localhost:9092"},
	})
```

## Producers

