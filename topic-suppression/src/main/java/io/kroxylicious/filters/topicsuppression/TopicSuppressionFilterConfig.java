package io.kroxylicious.filters.topicsuppression;

import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TopicSuppressionFilterConfig {

    private final List<String> denyTopics;
    private final List<Pattern> denyPatterns;
    private final List<String> allowTopics;
    private final List<Pattern> allowPatterns;

    @JsonCreator
    public TopicSuppressionFilterConfig(
            @JsonProperty("denyTopics") List<String> denyTopics,
            @JsonProperty("denyPatterns") List<String> denyPatterns,
            @JsonProperty("allowTopics") List<String> allowTopics,
            @JsonProperty("allowPatterns") List<String> allowPatterns) {

        this.denyTopics = denyTopics == null ? List.of() : List.copyOf(denyTopics);
        this.denyPatterns = denyPatterns == null ? List.of()
                : denyPatterns.stream().map(Pattern::compile).toList();
        this.allowTopics = allowTopics == null ? List.of() : List.copyOf(allowTopics);
        this.allowPatterns = allowPatterns == null ? List.of()
                : allowPatterns.stream().map(Pattern::compile).toList();
    }

    public List<String> denyTopics() {
        return denyTopics;
    }

    public List<Pattern> denyPatterns() {
        return denyPatterns;
    }

    public List<String> allowTopics() {
        return allowTopics;
    }

    public List<Pattern> allowPatterns() {
        return allowPatterns;
    }

    public boolean hasAllowRules() {
        return !allowTopics.isEmpty() || !allowPatterns.isEmpty();
    }

    public boolean hasDenyRules() {
        return !denyTopics.isEmpty() || !denyPatterns.isEmpty();
    }
}
