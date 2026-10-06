package io.kroxylicious.filters.metadataresponsefabrication;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import io.kroxylicious.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetadataResponseFabricationFilterTest {

    @Mock
    private FilterContext context;

    @Mock
    private RequestFilterResult requestFilterResult;

    @Mock
    private ResponseFilterResult responseFilterResult;

    private RequestHeaderData requestHeader;
    private ResponseHeaderData responseHeader;

    @BeforeEach
    void setUp() {
        requestHeader = new RequestHeaderData();
        responseHeader = new ResponseHeaderData();
    }

    private void stubForwardRequest() {
        when(context.forwardRequest(any(RequestHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(requestFilterResult));
    }

    private void stubForwardResponse() {
        when(context.forwardResponse(any(ResponseHeaderData.class), any(ApiMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(responseFilterResult));
    }

    private MetadataResponseFabricationFilter createFilter(String realPrefix, String fabricatedPrefix) {
        return new MetadataResponseFabricationFilter(
                new MetadataResponseFabricationFilterConfig(realPrefix, fabricatedPrefix));
    }

    private void sendNullTopicRequest(MetadataResponseFabricationFilter filter) {
        stubForwardRequest();
        MetadataRequestData request = new MetadataRequestData();
        request.setTopics(null);
        filter.onMetadataRequest((short) 9, requestHeader, request, context);
    }

    private void sendExplicitTopicRequest(MetadataResponseFabricationFilter filter, String... topics) {
        stubForwardRequest();
        MetadataRequestData request = new MetadataRequestData();
        for (String name : topics) {
            request.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(name));
        }
        filter.onMetadataRequest((short) 9, requestHeader, request, context);
    }

    private MetadataResponseData responseWithTopics(String... names) {
        MetadataResponseData response = new MetadataResponseData();
        for (String name : names) {
            MetadataResponseTopic topic = new MetadataResponseTopic()
                    .setName(name)
                    .setIsInternal(false)
                    .setTopicAuthorizedOperations(-2147483648);
            topic.partitions().add(new MetadataResponsePartition()
                    .setPartitionIndex(0)
                    .setLeaderId(1)
                    .setLeaderEpoch(0));
            response.topics().add(topic);
        }
        return response;
    }

    private List<String> forwardedResponseTopicNames() {
        ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
        verify(context).forwardResponse(any(), captor.capture());
        MetadataResponseData forwarded = (MetadataResponseData) captor.getValue();
        return forwarded.topics().stream().map(MetadataResponseTopic::name).toList();
    }

    @Nested
    class Fabrication {

        @Test
        void fabricatesTopicsForMatchingRealPrefix() {
            var filter = createFilter("A", "B");
            sendNullTopicRequest(filter);

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("topic-a", "A.topic-a", "A.topic-b"), context);

            assertThat(forwardedResponseTopicNames())
                    .containsExactly("topic-a", "A.topic-a", "B.topic-a", "A.topic-b", "B.topic-b");
        }

        @Test
        void fabricatedTopicsHaveEmptyPartitions() {
            var filter = createFilter("A", "B");
            sendNullTopicRequest(filter);

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-a"), context);

            ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
            verify(context).forwardResponse(any(), captor.capture());
            MetadataResponseData forwarded = (MetadataResponseData) captor.getValue();

            MetadataResponseTopic fabricated = forwarded.topics().stream()
                    .filter(t -> t.name().equals("B.topic-a"))
                    .findFirst().orElseThrow();
            assertThat(fabricated.partitions()).isEmpty();
        }

        @Test
        void doesNotFabricateForNonMatchingTopics() {
            var filter = createFilter("A", "B");
            sendNullTopicRequest(filter);

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("topic-a", "B.topic-a", "C.topic-a"), context);

            assertThat(forwardedResponseTopicNames())
                    .containsExactly("topic-a", "B.topic-a", "C.topic-a");
        }

        @Test
        void preservesOriginalTopicsUnmodified() {
            var filter = createFilter("A", "B");
            sendNullTopicRequest(filter);

            stubForwardResponse();
            MetadataResponseData response = responseWithTopics("A.topic-a");
            filter.onMetadataResponse((short) 9, responseHeader, response, context);

            ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
            verify(context).forwardResponse(any(), captor.capture());
            MetadataResponseData forwarded = (MetadataResponseData) captor.getValue();

            MetadataResponseTopic original = forwarded.topics().stream()
                    .filter(t -> t.name().equals("A.topic-a"))
                    .findFirst().orElseThrow();
            assertThat(original.partitions()).hasSize(1);
        }
    }

    @Nested
    class NullTopicGuard {

        @Test
        void skipsWhenRequestHadExplicitTopics() {
            var filter = createFilter("A", "B");
            sendExplicitTopicRequest(filter, "A.topic-a");

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-a"), context);

            assertThat(forwardedResponseTopicNames())
                    .containsExactly("A.topic-a");
        }
    }

    @Nested
    class Pipelining {

        @Test
        void pipelinedRequestsMatchResponsesInOrder() {
            var filter = createFilter("A", "B");

            sendNullTopicRequest(filter);
            sendExplicitTopicRequest(filter, "A.topic-a");

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-a"), context);
            List<String> firstResponse = forwardedResponseTopicNames();
            assertThat(firstResponse)
                    .as("First response (null-topic request) should fabricate")
                    .containsExactly("A.topic-a", "B.topic-a");

            reset(context);
            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-a"), context);
            List<String> secondResponse = forwardedResponseTopicNames();
            assertThat(secondResponse)
                    .as("Second response (explicit-topic request) should not fabricate")
                    .containsExactly("A.topic-a");
        }

        @Test
        void twoNullTopicRequestsBothFabricate() {
            var filter = createFilter("A", "B");

            sendNullTopicRequest(filter);
            sendNullTopicRequest(filter);

            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-a"), context);
            assertThat(forwardedResponseTopicNames())
                    .containsExactly("A.topic-a", "B.topic-a");

            reset(context);
            stubForwardResponse();
            filter.onMetadataResponse((short) 9, responseHeader,
                    responseWithTopics("A.topic-b"), context);
            assertThat(forwardedResponseTopicNames())
                    .containsExactly("A.topic-b", "B.topic-b");
        }
    }

    @Nested
    class Config {

        @Test
        void rejectsNullRealPrefix() {
            assertThatThrownBy(() -> new MetadataResponseFabricationFilterConfig(null, "B"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejectsEmptyRealPrefix() {
            assertThatThrownBy(() -> new MetadataResponseFabricationFilterConfig("", "B"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejectsNullFabricatedPrefix() {
            assertThatThrownBy(() -> new MetadataResponseFabricationFilterConfig("A", null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void rejectsEmptyFabricatedPrefix() {
            assertThatThrownBy(() -> new MetadataResponseFabricationFilterConfig("A", ""))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Factory {

        @Test
        void factoryCreatesWorkingFilter() {
            var factory = new MetadataResponseFabrication();
            var cfg = new MetadataResponseFabricationFilterConfig("A", "B");
            var init = factory.initialize(null, cfg);
            assertThat(init).isSameAs(cfg);
            assertThat(factory.createFilter(null, init))
                    .isInstanceOf(MetadataResponseFabricationFilter.class);
        }
    }
}
