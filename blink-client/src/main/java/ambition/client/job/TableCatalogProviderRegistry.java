package ambition.client.job;


import ambition.client.table.FlinkTableCatalogProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @Author: wpl
 */
public final class TableCatalogProviderRegistry {
    private static final Map<String, FlinkTableCatalogProvider> PROVIDERS;

    static {
        HashMap<String, FlinkTableCatalogProvider> providers = new HashMap<>();
        ServiceLoader<FlinkTableCatalogProvider> loaders =
                ServiceLoader.load(FlinkTableCatalogProvider.class);
        loaders.forEach(x -> providers.put(x.getType(), x));
        PROVIDERS = Collections.unmodifiableMap(providers);
    }

    private TableCatalogProviderRegistry() {
    }

    public static FlinkTableCatalogProvider getProvider(String type) {
        return PROVIDERS.get(type);
    }
}
