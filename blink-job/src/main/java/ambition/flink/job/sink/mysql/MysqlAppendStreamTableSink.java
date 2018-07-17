package ambition.flink.job.sink.mysql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author: wpl
 */
public class MysqlAppendStreamTableSink implements AppendStreamTableSink<Row> {

    public String[] fieldNames;
    public TypeInformation<?>[] fieldTypes;
    public final Properties props;

    public MysqlAppendStreamTableSink(Properties props) {
        this.props = props;
    }


    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        MysqlAppendStreamTableSink mysqlAppendStreamTableSink = new MysqlAppendStreamTableSink(props);
        mysqlAppendStreamTableSink.fieldNames = fieldNames;
        mysqlAppendStreamTableSink.fieldTypes = fieldTypes;
        return mysqlAppendStreamTableSink ;
    }
    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }
    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes());
    }
    @Override
    public void emitDataStream(DataStream<Row> rows) {
        rows.addSink(new MysqlSink(props, fieldNames, fieldTypes)).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
    }
}
