package ambition.client.table;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @Author: wpl
 */
public interface FlinkTableSinkProvider {
    String getType();

    AppendStreamTableSink<Row> getAppendStreamTableSink(ExternalCatalogTable table) throws IOException;

    UpsertStreamTableSink<Row> getUpsertStreamTableSink(ExternalCatalogTable table) throws IOException;

    BatchTableSink<Row> getBatchTableSink(ExternalCatalogTable table) throws IOException;
}
