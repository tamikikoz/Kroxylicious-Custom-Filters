# Kroxylicious Topic Suppression Filter — Implementation Spec

## 1. Background and problem

We run Kroxylicious (v0.24.0) as a Kafka protocol proxy in front of two regional
Kafka clusters ("cluster A" / region-1, "cluster B" / region-2). Clients reach
whichever cluster is currently live via SNI routing behind a DNS name managed by
PowerDNS, pointed at a Kubernetes LoadBalancer Service. During a region
migration, PowerDNS is repointed and the client's existing TCP connection to
Kroxylicious is severed; the client (kafkajs, Node.js) reconnects and is now
talking to the other physical cluster.

The two clusters are bidirectionally mirrored with MirrorMaker 2, using the
default `DefaultReplicationPolicy` naming convention: a topic `topic-a`
produced locally in region-1 is mirrored into cluster B as `A.topic-a`, and
`topic-a` produced locally in region-2 is mirrored into cluster A as
`B.topic-a`. So on cluster A, the valid topic names are `topic-a` (local) and
`B.topic-a` (mirror); on cluster B they are `topic-a` (local) and `A.topic-a`
(mirror). `A.topic-a` does not exist on cluster A, and `B.topic-a` does not
exist on cluster B — by design.

### The client-side bug this filter works around

The kafkajs consumer currently subscribes via a regex
(`^([^.]+\.)?topic-a$`). We traced kafkajs's source (`consumerGroup.js`,
`consumer/index.js`) and confirmed that for a regex subscription, the matched
topic list is computed **exactly once**, inside `subscribe()`, against
whatever topics exist on the cluster at that moment, and is then cached
(`this.topicsSubscribed`) for the lifetime of the `ConsumerGroup` instance.
Nothing in kafkajs — not reconnect handling, not rejoin, not
`metadataMaxAge`, not any timer — ever recomputes that match. So after a
region migration, the client keeps asking for the topic name that was valid
on the *previous* cluster, which does not exist on the new one, and silently
never picks up the correct topic.

### Why we can't just fix this in the client

Constraint: client applications cannot be modified beyond simple environment
variable changes. This must be solved entirely on the Kroxylicious/infra
side.

### Why a static topic list + filter, not client-side regex tuning

We tested subscribing kafkajs to a static, literal topic array
(`['topic-a', 'A.topic-a', 'B.topic-a']`) instead of a regex, reasoning that
a fixed superset of both regions' topic names would remain valid across a
migration without needing any re-evaluation. This does **not** work
out-of-the-box: kafkajs's `Cluster.addMultipleTargetTopics` /
`Cluster.refreshMetadata` path is not wrapped in kafkajs's retry helper, and
kafkajs's Metadata response parser
(`node_modules/kafkajs/src/protocol/requests/metadata/v0/response.js`)
throws a hard, uncaught `KafkaJSProtocolError` the moment *any* requested
topic in a `MetadataRequest` comes back with a non-zero error code
(confirmed in production: `UNKNOWN_TOPIC_OR_PARTITION`, code 3, for the topic
that doesn't exist on the connected cluster). This is not retried (confirmed:
`retriable: true` on the error is purely descriptive metadata on the error
class — it is not consulted anywhere on this call path) and is not
catchable via `consumer.on(consumer.events.CRASH, ...)` (confirmed: `CRASH`
is only emitted from inside the `Runner`'s `run()` loop machinery; a
rejection from `subscribe()` itself never reaches that code path). An
uncaught rejection here crashes the Node process.

The same unguarded `refreshMetadata()` call is also made by the consumer
group **leader** during `SYNC`/rejoin (`consumerGroup.js`, `[PRIVATE.SYNC]`),
so this isn't just a startup-time problem — it can also crash a client
mid-migration if it becomes group leader on a cluster where one of its
statically-subscribed topics doesn't exist.

**Conclusion driving this spec:** the client can keep a static topic list,
but the *Kafka broker must never be asked about a topic name that doesn't
exist on it*, from the client's point of view. That's what this filter does:
it strips the one known-invalid topic name out of every `MetadataRequest`
before it reaches the backend cluster, per virtual cluster, so kafkajs never
receives the error condition it cannot tolerate.

## 2. What this filter must do

- Implement `io.kroxylicious.proxy.filter.MetadataRequestFilter`, intercepting
  `MetadataRequest` (all API versions in use) between client and backend
  cluster.
- Be configured with a list of one or more topic names to suppress
  (`suppressedTopics`), configured **per virtual cluster** (cluster A's proxy
  instance suppresses `A.topic-a`; cluster B's suppresses `B.topic-a`).
- On each `MetadataRequest`: if the request's `topics` field is non-null
  (i.e. the client asked for specific topics, not "all topics"), remove any
  entries whose name is in the configured suppression list, then forward the
  mutated request unchanged otherwise.
- If `topics` is null (an "all topics" request), pass through unmodified —
  there's nothing to suppress; the broker's own catalog naturally excludes
  the nonexistent topic already.
- Do **not** touch `Produce`, `Fetch`, `OffsetFetch`, `OffsetCommit`, or any
  other request type. The consumer will never be assigned partitions for a
  topic it was never given metadata for, so it will never issue those
  requests for the suppressed topic — no other protocol surface should need
  changes. (This is a deliberate scope boundary vs. a full topic-renaming/
  alias filter, which would need to touch far more of the protocol and carry
  offset-routing correctness risk. We are explicitly not doing that here.)
- No topic renaming. Real topic names are preserved everywhere, for both
  regions. This keeps broker-side tooling (`kafka-topics.sh`,
  `kafka-consumer-groups.sh --describe`, lag monitors, ACLs, Schema
  Registry subject names) fully consistent with what the client sees — no
  translation table to maintain or get wrong.

## 3. Explicit non-goals

- This filter does **not** address MirrorMaker 2 consumer-offset checkpoint
  asymmetry (the fact that a consumer group's offsets only get
  auto-translated via MM2 checkpoints for the topic leg it was reading
  *directly from its origin cluster*, not the leg it was reading via
  mirror). That is a separate, known, accepted gap given our constraints —
  out of scope for this work.
- Not a general-purpose topic multi-tenancy/renaming filter.
- Not intended to auto-discover which topic is invalid per cluster — the
  suppression list is static, operator-configured per virtual cluster.

## 4. Deliverables

1. A standalone Maven project for the filter (generated from
   `kroxylicious-filter-archetype`), producing a JAR that can be placed on
   Kroxylicious's classpath / plugin directory.
2. Java source:
   - `TopicSuppressionFilter` — implements `MetadataRequestFilter`.
   - `TopicSuppressionFilterConfig` — Jackson-deserialized config POJO
     (`suppressedTopics: List<String>`).
   - `TopicSuppressionFilterFactory` — implements `FilterFactory`, wires
     config to filter instances.
   - `META-INF/services/io.kroxylicious.proxy.filter.FilterFactory` service
     file registering the factory.
3. Unit tests (using `kroxylicious-filter-test-support`) covering:
   - A `MetadataRequest` with an explicit topics list containing one
     suppressed and one non-suppressed topic → suppressed topic removed,
     other topic passed through unchanged, request forwarded (not
     short-circuited).
   - A `MetadataRequest` with `topics = null` ("all topics") → request
     passed through completely unmodified.
   - A `MetadataRequest` where none of the requested topics are in the
     suppression list → unmodified passthrough.
   - Config with an empty/absent `suppressedTopics` list → filter is a
     no-op passthrough (safe default).
   - Multiple suppressed topics configured → all matching entries removed.
4. Integration test (see §6 — this is the critical validation, not optional).
5. Two example YAML config snippets (one per virtual cluster / region) showing
   `filterDefinitions` and how the filter attaches to each `virtualClusters`
   entry, using the real backend `targetCluster`/SNI/TLS config already in
   use.
6. A short operator README covering build, deploy, and — critically — the
   known open risk in §7, with instructions for how to test it before
   production rollout.

## 5. API surface to implement against

Confirmed from the Kroxylicious developer guide (verify exact method names
against the `kroxylicious-api` version actually on the classpath — v0.24.0 —
since these interfaces can shift slightly between releases; treat what
follows as a strong starting point, not a guaranteed-exact signature):

```java
public interface MetadataRequestFilter extends Filter {
    CompletionStage<RequestFilterResult> onMetadataRequest(
        short apiVersion,
        RequestHeaderData header,
        MetadataRequestData request,
        FilterContext context);
}
```

Forwarding a mutated request is expected to go through a builder obtained
from `FilterContext`, roughly:

```java
context.requestFilterResultBuilder()
       .forwardRequest(header, request)
       .build();
```

`FilterFactory` implementations are discovered via `ServiceLoader` (no
`@Plugin` annotation in this framework version) and are expected to expose
`initialize(FilterFactoryContext, ConfigType)` and
`createFilter(FilterFactoryContext, ConfigType)` methods binding the
Jackson-deserialized config to filter instances. Config classes must be
Jackson-deserializable (constructor or setter-based, `@JsonCreator`/
`@JsonProperty` as needed).

**Action for Claude Code:** before writing final code, pull the actual
`kroxylicious-api` sources/javadoc for v0.24.0 (from the dependency jar on
the classpath once the archetype project is scaffolded, or from the
project's GitHub tag `v0.24.0`) and confirm the exact interface and builder
method names, rather than relying solely on the above. Adjust the draft
implementation in §8 accordingly — the logic/behavior described there should
not need to change, only exact method names if they differ.

## 6. Critical validation step — do this before considering the filter "done"

There is one behavior we have **not** empirically verified and could not
confirm from kafkajs source alone, and it determines whether this whole
approach actually works end-to-end:

The client's own `topicsSubscribed` state still contains the suppressed
topic name (e.g. `A.topic-a`), even though this filter stops the broker from
ever being asked about it. That means kafkajs's local `Cluster` object will
have **zero** cached partition metadata for that topic — not an error
response, just an absence. When this consumer instance is elected **group
leader**, `consumerGroup.js`'s `SYNC` step calls the partition assignor
(`assigner.assign({ members, topics: topicsSubscribed })`) using that full
subscribed-topics list, including the one with no known partitions.

We need to confirm empirically whether the assignor (and anything else in
that path) gracefully treats "subscribed topic with zero known partitions"
as "contribute zero partitions for it, proceed normally" — which is the
behavior this whole design depends on — or whether something throws because
partition metadata is unexpectedly absent for a topic in the subscription.

**Test plan:**

1. Stand up a local/test Kroxylicious instance with this filter configured
   to suppress a topic that genuinely does not exist on the backend test
   cluster.
2. Run a real kafkajs consumer (matching production's kafkajs version)
   subscribed with the static three-name array, connected through the
   filtered proxy, in a **single-member consumer group** (so this instance
   is guaranteed to be elected leader).
3. Confirm: consumer starts cleanly, no crash, and successfully receives
   partition assignments for the topics that *do* exist.
4. Force a rejoin (e.g. restart the broker's group coordinator, or bounce
   the consumer's connection) while the filter is active, and confirm the
   same clean behavior on rejoin — this is the leader-side `refreshMetadata`
   path specifically, not just the initial `subscribe()` path.
5. Repeat with a **multi-member** consumer group to confirm leader election
   doesn't change the outcome depending on which member becomes leader.

If any of these crash, this approach does not work as designed and we fall
back to the forced-restart-on-migration approach discussed separately
(out of scope for this filter, but keep in mind as the fallback).

## 7. Configuration reference

Per-virtual-cluster suppression list, example for cluster A's Kroxylicious
instance:

```yaml
filterDefinitions:
  - name: suppress-invalid-topic
    type: TopicSuppression
    config:
      suppressedTopics: ["A.topic-a"]

virtualClusters:
  - name: my-cluster
    filters: ["suppress-invalid-topic"]
    # existing targetCluster / SNI / TLS config unchanged
```

Cluster B's instance is identical except `suppressedTopics: ["B.topic-a"]`.

If/when more topics or more regions are added, this list grows accordingly —
note this approach requires knowing and enumerating the invalid names
up front per cluster; it does not auto-discover them.

## 8. Draft filter implementation (starting point for Claude Code)

```java
package com.yourorg.kroxylicious.filters;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class TopicSuppressionFilter implements MetadataRequestFilter {

    private final List<String> topicsToSuppress;

    public TopicSuppressionFilter(TopicSuppressionFilterConfig config) {
        this.topicsToSuppress = config.suppressedTopics();
    }

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(
            short apiVersion,
            RequestHeaderData header,
            MetadataRequestData request,
            FilterContext context) {

        if (request.topics() != null) {
            request.topics().removeIf(t -> topicsToSuppress.contains(t.name()));
        }

        return context.requestFilterResultBuilder()
                .forwardRequest(header, request)
                .build();
    }
}
```

```java
package com.yourorg.kroxylicious.filters;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicSuppressionFilterConfig {
    private final List<String> suppressedTopics;

    @JsonCreator
    public TopicSuppressionFilterConfig(
            @JsonProperty("suppressedTopics") List<String> suppressedTopics) {
        this.suppressedTopics = suppressedTopics == null ? List.of() : suppressedTopics;
    }

    public List<String> suppressedTopics() {
        return suppressedTopics;
    }
}
```

```java
package com.yourorg.kroxylicious.filters;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

public class TopicSuppressionFilterFactory
        implements FilterFactory<TopicSuppressionFilterConfig, TopicSuppressionFilterConfig> {

    @Override
    public TopicSuppressionFilterConfig initialize(FilterFactoryContext context,
                                                     TopicSuppressionFilterConfig config) {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, TopicSuppressionFilterConfig config) {
        return new TopicSuppressionFilter(config);
    }
}
```

`src/main/resources/META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`:
```
com.yourorg.kroxylicious.filters.TopicSuppressionFilterFactory
```

## 9. Acceptance criteria

- [ ] Project builds cleanly against Kroxylicious v0.24.0's `kroxylicious-api`.
- [ ] Unit tests in §4 pass.
- [ ] §6's integration validation passes for both single-member and
      multi-member consumer groups, including the rejoin scenario.
- [ ] A kafkajs consumer with the static three-topic array, run against a
      filtered proxy in front of a real two-cluster MM2 test setup, survives
      a simulated region migration (DNS/connection cutover) without
      crashing and without needing a process restart, and correctly resumes
      consuming from whichever two of the three topic names are valid on
      the cluster it lands on.
- [ ] `allowAutoTopicCreation` is confirmed `false` on the client regardless
      of this filter (defense in depth — not a substitute for it; ACLs
      blocking topic creation is not equivalent to this being disabled, and
      relying on ACLs instead of the explicit flag would turn a benign
      missing-topic condition into a fatal `TOPIC_AUTHORIZATION_FAILED`
      crash — do not rely on ACLs here).
- [ ] Operator README documents the per-cluster config difference clearly,
      so it isn't accidentally deployed with the same suppression list to
      both regions.

## 10. Known residual limitations (accepted, not to be "fixed" by this work)

- MM2 checkpoint offset-continuity asymmetry remains unresolved (see §3).
- This design requires manually maintaining the suppression list if regions
  or mirrored topics are added/renamed.
- If Kroxylicious's `MetadataRequestFilter`/`FilterContext` API changes in a
  future version, this filter needs to be revisited at upgrade time.
