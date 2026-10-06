package io.kroxylicious.filters.metadataresponsefabrication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetadataResponseFabricationFilterConfig {

    private final String realPrefix;
    private final String fabricatedPrefix;

    @JsonCreator
    public MetadataResponseFabricationFilterConfig(
            @JsonProperty("realPrefix") String realPrefix,
            @JsonProperty("fabricatedPrefix") String fabricatedPrefix) {
        if (realPrefix == null || realPrefix.isEmpty()) {
            throw new IllegalArgumentException("realPrefix must be non-empty");
        }
        if (fabricatedPrefix == null || fabricatedPrefix.isEmpty()) {
            throw new IllegalArgumentException("fabricatedPrefix must be non-empty");
        }
        this.realPrefix = realPrefix;
        this.fabricatedPrefix = fabricatedPrefix;
    }

    public String realPrefix() {
        return realPrefix;
    }

    public String fabricatedPrefix() {
        return fabricatedPrefix;
    }
}
