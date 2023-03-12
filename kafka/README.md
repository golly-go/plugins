
# Kafka

## Consumers

### Config


Default Configuration:

```
	config.SetDefault("kafka.consumer", map[string]interface{}{
		"workers": map[string]interface{}{
			"min": 1,
			"max": 25,
			// "buffer": 2_500,
			"buffer": 50,
		},
		"bytes": map[string]int{
			"min": 10_000, // 10e3,
			"max": 10_000_000,
		},
		"partition": 0,
		"wait":      "50ms",
		"brokers":   []string{"localhost:9092"},
		"group_id":  "default-group",
	})
```

## Producers

