package ambition.client.table;

import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @Author: wpl
 */
public interface FlinkTableCatalog extends ExternalCatalog {
}
