package kafka

import (
	"strings"
	"unicode"
)

// sanitizeTopic replaces characters that are unsafe for certain Kafka providers (e.g., MSK)
// Current policy: replace '/' and '.' with '-'.
func sanitizeTopic(topic string) string {
	var b strings.Builder
	b.Grow(len(topic)) // lower bound; may grow if case fold expands
	for _, r := range topic {
		switch r {
		case '/':
			b.WriteByte('-')
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}

	return b.String()
}

// sanitizeTopics accepts either a single topic (string) or a list ([]string)
// and returns a sanitized []string. Returns nil for unsupported types.
func sanitizeTopics(topics any) []string {
	switch v := topics.(type) {
	case string:
		return []string{sanitizeTopic(v)}
	case []string:
		if len(v) == 0 {
			return []string{}
		}
		out := make([]string, len(v))
		for i, s := range v {
			out[i] = sanitizeTopic(s)
		}
		return out
	default:
		return nil
	}
}
