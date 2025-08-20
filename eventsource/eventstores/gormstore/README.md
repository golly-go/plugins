# GormStore


Updating to v8 bus centric model requires an update to the Events to populate the topic:

```
UPDATE events
SET topic = lower(regexp_replace(aggregate_type, '[./]', '-', 'g'))
WHERE topic IS NULL OR topic = '';
```