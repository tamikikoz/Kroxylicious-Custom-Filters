# Kroxylicious Topic Suppression Filter

A Kroxylicious (v0.24.0) filter that strips configured topic names from
`MetadataRequest` messages before they reach the backend Kafka cluster. This
prevents kafkajs consumers with static topic subscriptions from crashing with
`UNKNOWN_TOPIC_OR_PARTITION` when a topic name exists only on the other
cluster in a MirrorMaker 2 bidirectional replication setup.

## Build

```bash
mvn clean package
```

The resulting JAR is at `target/topic-suppression-filter-1.0.0-SNAPSHOT.jar`.

## Deploy

Copy the JAR into your Kroxylicious plugin directory (typically
`/opt/kroxylicious/plugins/` or wherever your deployment's classpath is
configured).

## Configuration

The filter has four optional fields — use any combination:

| Field | Type | Description |
|---|---|---|
| `denyTopics` | `List<String>` | Exact topic names to suppress |
| `denyPatterns` | `List<String>` | Java regex patterns — topics matching any pattern are suppressed (full match) |
| `allowTopics` | `List<String>` | Exact topic names to allow through |
| `allowPatterns` | `List<String>` | Java regex patterns — only topics matching a pattern are let through (full match) |

### Evaluation logic

A topic is **suppressed** (removed from the MetadataRequest) if:
- It matches any **deny** rule (exact name or regex), **OR**
- Allow rules are configured and it does **not** match any **allow** rule

Deny takes precedence: a topic matching both a deny and an allow rule is suppressed.

If no rules are configured, the filter is a no-op.

### Examples

**Deny specific topics** per cluster:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      denyTopics:
        - "A.topic-a"
```

**Deny by regex** — suppress all mirror-prefixed topics from a region:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      denyPatterns:
        - "^A\\..*"
```

**Allowlist by exact names**:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      allowTopics:
        - "topic-a"
        - "B.topic-a"
```

**Allowlist by pattern**:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      allowPatterns:
        - "^topic-.*"
        - "^B\\..*"
```

**Combined deny + allow**:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      denyPatterns:
        - "^A\\..*"
      allowPatterns:
        - "^topic-.*"
        - "^[AB]\\.topic-.*"
```

Then reference the filter in your virtual cluster:

```yaml
virtualClusters:
  my-cluster:
    filters:
      - suppress-invalid-topic
    # ... existing targetCluster / TLS config unchanged
```

See `examples/` for complete config files.

## How it works

1. The filter intercepts only `MetadataRequest` messages (all API versions).
2. If the request contains an explicit topics list, each topic is evaluated
   against the deny/allow rules and removed if suppressed.
3. If the request is an "all topics" request (`topics: null`), it passes
   through unmodified — the broker's own catalog naturally excludes
   non-existent topics.
4. No other request types are touched. The consumer will never be assigned
   partitions for a topic it never received metadata for.

## Known open risk — validate before production

**You must run the integration test described in the spec (§6) before
deploying to production.** The critical assumption is that kafkajs's partition
assignor gracefully treats "subscribed topic with zero known partitions" as
"contribute zero partitions, proceed normally." If instead something throws
when partition metadata is absent for a subscribed topic, this approach does
not work.

Test plan summary:

1. Run Kroxylicious with this filter suppressing a genuinely non-existent topic.
2. Connect a kafkajs consumer (matching your production version) with the
   static three-topic array through the filtered proxy, in a **single-member**
   consumer group (guaranteeing leader election).
3. Verify: consumer starts, no crash, correct partition assignments for valid topics.
4. Force a rejoin (bounce the connection) and verify the same clean behavior.
5. Repeat with a **multi-member** group.

## Deployment checklist

- [ ] Ensure deny/allow rules are set differently per cluster — deploying
      the same config to both regions will suppress a valid topic on one side.
- [ ] Confirm `allowAutoTopicCreation: false` on the kafkajs client config
      (defense in depth — ACLs alone are not sufficient).
- [ ] Run the §6 integration test against your actual kafkajs version.
- [ ] Monitor consumer group lag after deployment to confirm consumption
      is healthy.

## Limitations

- MM2 checkpoint offset-continuity asymmetry is not addressed by this filter.
- Rules are static; adding/renaming mirrored topics requires a config update
  and Kroxylicious restart (regex patterns reduce this burden).
- Tied to Kroxylicious v0.24.0 API; may need adjustment on upgrade.
