package ambition.flink.job.sink.mysql;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import ambition.client.table.FlinkTableSinkProvider;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: wpl
 */
public class MysqlTableSink implements FlinkTableSinkProvider {

    private static final String TYPE = "mysql";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public AppendStreamTableSink<Row> getAppendStreamTableSink(ExternalCatalogTable table) throws IOException {
        DescriptorProperties params = new DescriptorProperties(true);
        table.addProperties(params);
        Properties props = new Properties();
        props.putAll(params.getPrefix(TYPE));


        return new MysqlAppendStreamTableSink(props);
    }

    @Override
    public UpsertStreamTableSink<Row> getUpsertStreamTableSink(ExternalCatalogTable table) throws IOException {
        DescriptorProperties params = new DescriptorProperties(true);
        table.addProperties(params);
        Properties props = new Properties();
        props.putAll(params.getPrefix(TYPE));

        return new MysqlUpsertStreamTableSink(props);
    }

    @Override
    public BatchTableSink<Row> getBatchTableSink(ExternalCatalogTable table) throws IOException {
        return null;
    }
}
