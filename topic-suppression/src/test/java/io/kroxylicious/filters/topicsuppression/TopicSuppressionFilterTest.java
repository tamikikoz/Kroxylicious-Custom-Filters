package io.kroxylicious.filters.topicsuppression;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicSuppressionFilterTest {

    @Mock
    private FilterContext context;

    @Mock
    private RequestFilterResult filterResult;

    private RequestHeaderData header;

    @BeforeEach
    void setUp() {
        header = new RequestHeaderData();
    }

    private void stubForward() {
        when(context.forwardRequest(any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(filterResult));
    }

    private MetadataRequestData requestWithTopics(String... names) {
        MetadataRequestData request = new MetadataRequestData();
        for (String name : names) {
            request.topics().add(new MetadataRequestTopic().setName(name));
        }
        return request;
    }

    private List<String> forwardedTopicNames() {
        ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
        verify(context).forwardRequest(any(), captor.capture());
        MetadataRequestData forwarded = (MetadataRequestData) captor.getValue();
        if (forwarded.topics() == null) {
            return null;
        }
        return forwarded.topics().stream().map(MetadataRequestTopic::name).toList();
    }

    private TopicSuppressionFilterConfig config(
            List<String> denyTopics, List<String> denyPatterns,
            List<String> allowTopics, List<String> allowPatterns) {
        return new TopicSuppressionFilterConfig(denyTopics, denyPatterns, allowTopics, allowPatterns);
    }

    @Nested
    class DenyTopics {

        @Test
        void exactNameSuppressed() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of("A.topic-a"), null, null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }

        @Test
        void multipleExactNamesSuppressed() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of("A.topic-a", "A.topic-b"), null, null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "topic-b", "A.topic-b", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "topic-b", "B.topic-a");
        }

        @Test
        void noMatchPassesThrough() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of("A.topic-a"), null, null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }
    }

    @Nested
    class DenyPatterns {

        @Test
        void wildcardPatternSuppresses() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(null, List.of("^A\\..*"), null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "A.topic-b", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }

        @Test
        void multipleDenyPatterns() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(null, List.of("^A\\..*", "^B\\..*"), null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a");
        }
    }

    @Nested
    class DenyTopicsAndPatternsCombined {

        @Test
        void bothDenyMechanismsApply() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of("exact-deny"), List.of("^A\\..*"), null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "exact-deny", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }
    }

    @Nested
    class AllowTopics {

        @Test
        void onlyAllowedTopicsPassThrough() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(null, null, List.of("topic-a", "B.topic-a"), null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }
    }

    @Nested
    class AllowPatterns {

        @Test
        void wildcardAllowPattern() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(null, null, null, List.of("^topic-.*", "^B\\..*")));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }
    }

    @Nested
    class DenyAndAllowCombined {

        @Test
        void denyTakesPrecedenceOverAllow() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(
                    List.of("B.topic-a"), null,
                    List.of("topic-a", "B.topic-a"), null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a");
        }

        @Test
        void denyPatternPlusAllowPattern() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(
                    null, List.of("^A\\..*"),
                    null, List.of("^topic-.*", "^[AB]\\.topic-.*")));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a", "B.topic-a", "__consumer_offsets"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "B.topic-a");
        }
    }

    @Nested
    class EdgeCases {

        @Test
        void nullTopicsListPassedThroughUnmodified() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of("A.topic-a"), null, null, null));
            MetadataRequestData request = new MetadataRequestData();
            request.setTopics(null);
            filter.onMetadataRequest((short) 9, header, request, context);
            assertThat(forwardedTopicNames()).isNull();
        }

        @Test
        void noRulesConfiguredIsNoOp() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(null, null, null, null));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "A.topic-a");
        }

        @Test
        void emptyListsIsNoOp() {
            stubForward();
            var filter = new TopicSuppressionFilter(config(List.of(), List.of(), List.of(), List.of()));
            filter.onMetadataRequest((short) 9, header,
                    requestWithTopics("topic-a", "A.topic-a"), context);
            assertThat(forwardedTopicNames()).containsExactly("topic-a", "A.topic-a");
        }

        @Test
        void factoryCreatesWorkingFilter() {
            TopicSuppression factory = new TopicSuppression();
            var cfg = config(List.of("A.topic-a"), null, null, null);
            var init = factory.initialize(null, cfg);
            assertThat(init).isSameAs(cfg);
            assertThat(factory.createFilter(null, init)).isInstanceOf(TopicSuppressionFilter.class);
        }
    }
}
