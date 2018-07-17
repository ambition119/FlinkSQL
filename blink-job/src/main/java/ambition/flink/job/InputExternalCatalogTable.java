package ambition.flink.job;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import java.util.Map;

/**
 * @Author: wpl
 */
public class InputExternalCatalogTable extends ExternalCatalogTable {
    public InputExternalCatalogTable(Map<String,String> descriptor) {
        super(false,true,false,true,descriptor);
    }
}
