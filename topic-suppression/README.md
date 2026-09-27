# Kroxylicious Topic Suppression Filter

One of the filters in this repo — see the [repo-level README](../README.md) for
build/CI conventions shared across all filters here.

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

Kroxylicious's startup script only scans **subdirectories** of
`/opt/kroxylicious/classpath-plugins/` (not the directory itself) and adds
each subdirectory's contents to the classpath. Place the jar in its own
subdirectory, e.g.:

```
/opt/kroxylicious/classpath-plugins/topic-suppression/topic-suppression-filter-1.0.0-SNAPSHOT.jar
```

A jar placed directly in `classpath-plugins/` (no subdirectory) is silently
**not** picked up. This mechanism is explicitly labelled "Alpha" by the
Kroxylicious project - it logs a warning at startup and may change in a
future release; check that warning is present at boot as confirmation the
plugin loaded, e.g.:

```bash
docker logs <container> | grep -i "classpath-plugins\|TopicSuppression"
```

If deploying via Kubernetes/OpenShift Helm (no Operator), mount the jar via
a `ConfigMap` (`binaryData`, base64-encoded) at that subdirectory path rather
than baking it into a custom image, unless you'd rather maintain your own
image build.

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

## Known open risk — validate against your own kafkajs version before production

The critical assumption is that kafkajs's partition assignor gracefully treats
"subscribed topic with zero known partitions" as "contribute zero partitions,
proceed normally." **This has been empirically confirmed for the single-member
(guaranteed group-leader) case** — see `kafkajs-integration-test/` for a real
kafkajs v2.2.4 client run through a live filtered proxy: initial subscribe,
leader election, real message consumption, and a forced rejoin (exercising the
leader-side `refreshMetadata` path specifically) all completed cleanly, with
the suppressed topic simply absent from `memberAssignment` - no crash, no
thrown error.

**Not yet run: the multi-member group case** (spec §6, step 5) - confirming
the outcome doesn't depend on which member happens to be elected leader.
Run `kafkajs-integration-test/` (extended to multiple consumer instances in
the same group) against your own kafkajs version before production rollout,
particularly if it differs from v2.2.4.

## Deployment checklist

- [ ] Ensure deny/allow rules are set differently per cluster — deploying
      the same config to both regions will suppress a valid topic on one side.
- [ ] Confirm `allowAutoTopicCreation: false` on the kafkajs client config
      (defense in depth — ACLs alone are not sufficient).
- [ ] Run `kafkajs-integration-test/` against your actual kafkajs version
      (single-member case already validated here; extend to multi-member
      before relying on this in production).
- [ ] Monitor consumer group lag after deployment to confirm consumption
      is healthy.

## Limitations

- MM2 checkpoint offset-continuity asymmetry is not addressed by this filter.
- Rules are static; adding/renaming mirrored topics requires a config update
  and Kroxylicious restart (regex patterns reduce this burden).
- Tied to Kroxylicious v0.24.0 API; may need adjustment on upgrade.
