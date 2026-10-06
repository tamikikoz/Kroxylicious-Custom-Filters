package io.kroxylicious.filters.metadataresponsefabrication;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = MetadataResponseFabricationFilterConfig.class)
public class MetadataResponseFabrication
        implements FilterFactory<MetadataResponseFabricationFilterConfig, MetadataResponseFabricationFilterConfig> {

    @Override
    public MetadataResponseFabricationFilterConfig initialize(FilterFactoryContext context,
                                                              MetadataResponseFabricationFilterConfig config)
            throws PluginConfigurationException {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context,
                               MetadataResponseFabricationFilterConfig config) {
        return new MetadataResponseFabricationFilter(config);
    }
}
