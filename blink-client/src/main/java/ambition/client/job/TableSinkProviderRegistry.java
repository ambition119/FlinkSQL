package ambition.client.job;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import ambition.client.table.FlinkTableSinkProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @Author: wpl
 */
public final class TableSinkProviderRegistry {
    private static final Map<String, FlinkTableSinkProvider> PROVIDERS;
    static {
        HashMap<String, FlinkTableSinkProvider> providers = new HashMap<>();
        ServiceLoader<FlinkTableSinkProvider> loaders =
                ServiceLoader.load(FlinkTableSinkProvider.class);
        loaders.forEach(x -> providers.put(x.getType(), x));
        PROVIDERS = Collections.unmodifiableMap(providers);
    }

    private TableSinkProviderRegistry() {
    }

    public static FlinkTableSinkProvider getProvider(ExternalCatalogTable table) {
        DescriptorProperties properties = new DescriptorProperties(true);
        table.addProperties(properties);
        String connectorType = properties.getString(ConnectorDescriptorValidator.CONNECTOR_TYPE());
        return PROVIDERS.get(connectorType);
    }

}
