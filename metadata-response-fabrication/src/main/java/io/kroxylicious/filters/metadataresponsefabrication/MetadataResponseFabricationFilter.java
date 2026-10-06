package io.kroxylicious.filters.metadataresponsefabrication;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import io.kroxylicious.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class MetadataResponseFabricationFilter
        implements MetadataRequestFilter, MetadataResponseFilter {

    private final String realPrefixDot;
    private final String fabricatedPrefixDot;
    private final Deque<Boolean> nullTopicRequests = new ArrayDeque<>();

    public MetadataResponseFabricationFilter(MetadataResponseFabricationFilterConfig config) {
        this.realPrefixDot = config.realPrefix() + ".";
        this.fabricatedPrefixDot = config.fabricatedPrefix() + ".";
    }

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(
            short apiVersion,
            RequestHeaderData header,
            MetadataRequestData request,
            FilterContext context) {
        nullTopicRequests.addLast(request.topics() == null);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(
            short apiVersion,
            ResponseHeaderData header,
            MetadataResponseData response,
            FilterContext context) {

        boolean wasNullTopicRequest = !nullTopicRequests.isEmpty()
                && nullTopicRequests.pollFirst();

        if (wasNullTopicRequest) {
            MetadataResponseTopicCollection merged = new MetadataResponseTopicCollection();
            for (MetadataResponseTopic topic : response.topics()) {
                merged.add(topic.duplicate());
                if (topic.name().startsWith(realPrefixDot)) {
                    String baseName = topic.name().substring(realPrefixDot.length());
                    String fabricatedName = fabricatedPrefixDot + baseName;
                    MetadataResponseTopic fake = new MetadataResponseTopic()
                            .setName(fabricatedName)
                            .setIsInternal(false)
                            .setTopicAuthorizedOperations(topic.topicAuthorizedOperations());
                    merged.add(fake);
                }
            }
            response.setTopics(merged);
        }

        return context.forwardResponse(header, response);
    }
}
