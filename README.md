# Kroxylicious Custom Filters

Custom [Kroxylicious](https://kroxylicious.io/) filter plugins, built against
`kroxylicious-api` v0.24.0. Each filter is a self-contained, standalone Maven
project living in its own top-level folder — its own `pom.xml`, source, tests,
examples, and README.

## Repo layout convention

```
<filter-name>/
├── README.md          <- filter-specific: what it does, config reference, usage
├── pom.xml
├── src/
│   ├── main/java/...
│   └── test/java/...
├── examples/           <- complete example proxy-config.yaml snippets (optional)
└── ...                 <- anything else specific to that filter (integration
                            tests, design spec, etc.)
```

Adding a new filter means adding a new top-level folder in this same shape —
nothing else in this repo needs to change to pick it up (see CI, below).

## Filters in this repo

- [`topic-suppression/`](topic-suppression/README.md) — strips configured
  topic names from `MetadataRequest`s, so kafkajs clients with static topic
  subscriptions never see `UNKNOWN_TOPIC_OR_PARTITION` for a topic that only
  exists on the other side of a MirrorMaker 2 bidirectional replication setup.

## Build

Every filter builds independently:

```bash
cd <filter-name>
mvn clean package
```

The CI (`.gitlab-ci.yml`) builds and tests **every** top-level folder with a
`pom.xml` automatically (`for pom in */pom.xml`) — no per-filter CI config
needed.

## Deploy

All filters here are loaded via Kroxylicious's `classpath-plugins` mechanism
(explicitly labelled "Alpha" by the Kroxylicious project — it may change in a
future release). The proxy's startup script only scans **subdirectories** of
`/opt/kroxylicious/classpath-plugins/`, adding each one's contents to the
classpath — a jar dropped directly in `classpath-plugins/` with no
subdirectory is silently never loaded:

```
/opt/kroxylicious/classpath-plugins/<filter-name>/<filter-name>-*.jar
```

Confirm a plugin loaded by checking for the Alpha warning (and the absence of
an "Unknown plugin instance" error) at proxy startup:

```bash
docker logs <container> | grep -i "classpath-plugins"
```

`Dockerfile` here builds an image with **every** filter's jar baked in
(`COPY jars/*.jar /opt/kroxylicious/classpath-plugins/custom-filters/`) —
built by the CI's `build-jars` stage, which collects every filter's jar into
a shared `jars/` directory first. If you only want one specific filter in
your deployment, mount its jar individually instead of using this image as-is.

## Wiring a filter into your Kroxylicious config

Every filter follows the same two-step pattern — see each filter's own
README for its specific `type:` name and config fields:

```yaml
filterDefinitions:
  - name: my-filter-instance
    type: <FilterName>
    config:
      # filter-specific config - see that filter's README

virtualClusters:
  - name: my-cluster
    # ...existing targetCluster / gateways / tls unchanged...
    filters:
      - my-filter-instance
```

## Contributing a new filter

1. New top-level folder, matching the layout convention above.
2. Standalone Maven project depending on `kroxylicious-api` (`provided`
   scope — it's already on the proxy's runtime classpath, don't bundle it).
3. `FilterFactory` implementation named to match its intended `type:` value
   directly (e.g. a class named `MyFilter`, not `MyFilterFactory`) —
   matches Kroxylicious's own convention (`Authorization`, `ProtocolLogger`,
   etc. — no `Filter`/`FilterFactory` suffix on any of their built-in ones).
4. `@Plugin(configType = ...)` on the factory class, `META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`
   registering it.
5. A filter-specific README covering config reference and usage.
