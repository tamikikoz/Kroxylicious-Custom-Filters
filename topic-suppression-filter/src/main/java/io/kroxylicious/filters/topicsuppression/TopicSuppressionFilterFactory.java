package io.kroxylicious.filters.topicsuppression;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = TopicSuppressionFilterConfig.class)
public class TopicSuppressionFilterFactory
        implements FilterFactory<TopicSuppressionFilterConfig, TopicSuppressionFilterConfig> {

    @Override
    public TopicSuppressionFilterConfig initialize(FilterFactoryContext context,
                                                   TopicSuppressionFilterConfig config)
            throws PluginConfigurationException {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context,
                               TopicSuppressionFilterConfig config) {
        return new TopicSuppressionFilter(config);
    }
}
