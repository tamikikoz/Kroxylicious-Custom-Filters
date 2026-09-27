package io.kroxylicious.filters.topicsuppression;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class TopicSuppressionFilter implements MetadataRequestFilter {

    private final Set<String> denyTopics;
    private final List<Pattern> denyPatterns;
    private final Set<String> allowTopics;
    private final List<Pattern> allowPatterns;
    private final boolean hasAllowRules;
    private final boolean hasDenyRules;

    public TopicSuppressionFilter(TopicSuppressionFilterConfig config) {
        this.denyTopics = Set.copyOf(config.denyTopics());
        this.denyPatterns = config.denyPatterns();
        this.allowTopics = Set.copyOf(config.allowTopics());
        this.allowPatterns = config.allowPatterns();
        this.hasAllowRules = config.hasAllowRules();
        this.hasDenyRules = config.hasDenyRules();
    }

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(
            short apiVersion,
            RequestHeaderData header,
            MetadataRequestData request,
            FilterContext context) {

        if (request.topics() != null && (hasDenyRules || hasAllowRules)) {
            request.topics().removeIf(t -> shouldSuppress(t.name()));
        }

        return context.forwardRequest(header, request);
    }

    private boolean shouldSuppress(String topicName) {
        if (hasDenyRules && isDenied(topicName)) {
            return true;
        }
        if (hasAllowRules && !isAllowed(topicName)) {
            return true;
        }
        return false;
    }

    private boolean isDenied(String topicName) {
        if (denyTopics.contains(topicName)) {
            return true;
        }
        for (Pattern p : denyPatterns) {
            if (p.matcher(topicName).matches()) {
                return true;
            }
        }
        return false;
    }

    private boolean isAllowed(String topicName) {
        if (allowTopics.contains(topicName)) {
            return true;
        }
        for (Pattern p : allowPatterns) {
            if (p.matcher(topicName).matches()) {
                return true;
            }
        }
        return false;
    }
}
