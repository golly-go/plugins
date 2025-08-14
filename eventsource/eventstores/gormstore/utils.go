package gormstore

import (
	"hash/fnv"

	"github.com/segmentio/encoding/json"

	"github.com/golly-go/plugins/eventsource"
	"gorm.io/gorm"
)

// mapToDB converts an eventsource.Event to a database-compatible Event.
func mapToDB(evt *eventsource.Event) (Event, error) {
	var err error
	ret := Event{Event: *evt}

	ret.RawMetadata.RawMessage, err = json.Marshal(evt.Metadata)
	if err != nil {
		return ret, err
	}

	ret.RawData.RawMessage, err = json.Marshal(evt.Data)
	if err != nil {
		return ret, err
	}

	return ret, nil
}

// mapBatchToDB maps a batch of events for batch insertion.
func mapBatchToDB(events []*eventsource.Event) ([]Event, error) {
	var result []Event
	for _, evt := range events {
		mapped, err := mapToDB(evt)
		if err != nil {
			return nil, err
		}
		result = append(result, mapped)
	}
	return result, nil
}

func applyFilters(query *gorm.DB, filters ...eventsource.EventFilter) *gorm.DB {
	if len(filters) > 0 {
		filter := filters[0]

		if filter.AggregateID != "" {
			query = query.Where("aggregate_id = ?", filter.AggregateID)
		}
		if filter.AggregateType != "" {
			query = query.Where("aggregate_type = ?", filter.AggregateType)
		}

		if len(filter.AggregateTypes) > 0 {
			query = query.Where("aggregate_type IN ?", filter.AggregateTypes)
		}

		if len(filter.EventType) > 0 {
			query = query.Where("type IN ?", filter.EventType)
		}
		if filter.FromVersion > 0 {
			query = query.Where("version >= ?", filter.FromVersion)
		}
		if filter.ToVersion > 0 {
			query = query.Where("version <= ?", filter.ToVersion)
		}
		if !filter.FromTime.IsZero() {
			query = query.Where("timestamp >= ?", filter.FromTime)
		}
		if !filter.ToTime.IsZero() {
			query = query.Where("timestamp <= ?", filter.ToTime)
		}
		if filter.FromGlobalVersion > 0 {
			query = query.Where("global_version >= ", filter.FromGlobalVersion)
		}
		if filter.ToGlobalVersion > 0 {
			query = query.Where("global_version <= ", filter.ToGlobalVersion)
		}

		if filter.Limit > 0 {
			query = query.Limit(filter.Limit)
		}

	}

	return query
}

func aggregatorLockKey(aggType, aggID string) int64 {
	h := fnv.New64a()

	h.Write([]byte(aggType))
	h.Write([]byte("|"))
	h.Write([]byte(aggID))

	return int64(h.Sum64())
}
