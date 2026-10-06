# Kroxylicious Metadata Response Fabrication Filter

One of the filters in this repo — see the [repo-level README](../README.md) for
build/CI conventions shared across all filters here.

A Kroxylicious (v0.24.0) filter that injects fabricated topic entries into
`MetadataResponse` messages for null-topic (all-topics) metadata requests.
In a bidirectional MirrorMaker 2 setup, each cluster has mirrored topics with
a prefix (e.g., Cluster B has `A.topic-a`). kafkajs clients subscribe via
regex and resolve topics against a null-topic MetadataResponse. After
switchover, `topicsSubscribed` is stale because kafkajs doesn't re-evaluate
regex subscriptions. This filter ensures the regex captures all mirror
variants at subscribe time by fabricating the "other side" prefix entries.

## Build

```bash
mvn clean package
```

The resulting JAR is at `target/metadata-response-fabrication-filter-1.0.0-SNAPSHOT.jar`.

## Deploy

Kroxylicious's startup script only scans **subdirectories** of
`/opt/kroxylicious/classpath-plugins/` (not the directory itself) and adds
each subdirectory's contents to the classpath. Place the jar in its own
subdirectory, e.g.:

```
/opt/kroxylicious/classpath-plugins/metadata-response-fabrication/metadata-response-fabrication-filter-1.0.0-SNAPSHOT.jar
```

A jar placed directly in `classpath-plugins/` (no subdirectory) is silently
**not** picked up. This mechanism is explicitly labelled "Alpha" by the
Kroxylicious project — it logs a warning at startup and may change in a
future release; check that warning is present at boot as confirmation the
plugin loaded, e.g.:

```bash
docker logs <container> | grep -i "classpath-plugins\|MetadataResponseFabrication"
```

If deploying via Kubernetes/OpenShift Helm (no Operator), mount the jar via
a `ConfigMap` (`binaryData`, base64-encoded) at that subdirectory path rather
than baking it into a custom image, unless you'd rather maintain your own
image build.

## Configuration

| Field | Type | Description |
|---|---|---|
| `realPrefix` | `String` (required) | The mirror prefix that exists on this cluster (e.g., `"A"` on Cluster B) |
| `fabricatedPrefix` | `String` (required) | The prefix to fabricate (e.g., `"B"` on Cluster B) |

### Example

On **Cluster B**, real mirrored topics have the prefix `A` (e.g., `A.topic-a`).
To fabricate `B.topic-a` entries so kafkajs regex subscriptions see both
prefixes:

```yaml
filterDefinitions:
  - name: fabricate-mirror-metadata
    type: MetadataResponseFabrication
    config:
      realPrefix: "A"
      fabricatedPrefix: "B"
```

On **Cluster A** (where real mirrored topics have prefix `B`):

```yaml
filterDefinitions:
  - name: fabricate-mirror-metadata
    type: MetadataResponseFabrication
    config:
      realPrefix: "B"
      fabricatedPrefix: "A"
```

Then reference the filter in your virtual cluster:

```yaml
virtualClusters:
  my-cluster:
    filters:
      - fabricate-mirror-metadata
    # ... existing targetCluster / TLS config unchanged
```

## How it works

1. The filter implements both `MetadataRequestFilter` and
   `MetadataResponseFilter` on the same instance (per-connection).
2. On each `MetadataRequest`, it records whether the request is a null-topic
   request (`request.topics() == null`).
3. On the corresponding `MetadataResponse`, if the request was null-topic:
   - For each topic matching `<realPrefix>.X`, a fabricated
     `<fabricatedPrefix>.X` entry is added with **empty partition metadata**
     (no `MetadataResponsePartition` entries).
   - Original topics are preserved unmodified.
4. If the request had an explicit topics list, the response passes through
   unchanged.
5. No other request/response types are touched.

The fabricated entries have zero partitions, so no consumer will be assigned
partitions for them — they exist solely to satisfy kafkajs's regex topic
resolution so that `topicsSubscribed` includes all mirror variants.

## Limitations

- Both `realPrefix` and `fabricatedPrefix` are required and must be non-empty.
- The prefix matching uses a simple `<prefix>.` string prefix check (dot
  separator), matching MirrorMaker 2's default naming convention.
- Rules are static; changing prefixes requires a config update and
  Kroxylicious restart.
- Tied to Kroxylicious v0.24.0 API; may need adjustment on upgrade.
