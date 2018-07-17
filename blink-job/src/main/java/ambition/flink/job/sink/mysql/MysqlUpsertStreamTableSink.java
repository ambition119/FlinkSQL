package ambition.flink.job.sink.mysql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author: wpl
 */
public class MysqlUpsertStreamTableSink implements UpsertStreamTableSink<Row> {
    public String[] fieldNames;
    public TypeInformation<?>[] fieldTypes;
    public String[] keys;
    public Boolean appendOnly;

    public final Properties props;

    public MysqlUpsertStreamTableSink(Properties props) {
        this.props = props;
    }

    @Override
    public void setKeyFields(String[] keys) {
        this.keys = keys;
    }

    @Override
    public void setIsAppendOnly(Boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        if (appendOnly){
            dataStream.addSink(new MysqlAppendTableSink(props, fieldNames, fieldTypes)).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
        } else {
            dataStream.addSink(new MysqlUpsertTableSink(props, fieldNames, fieldTypes)).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
        }
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(Types.BOOLEAN(),new RowTypeInfo(getFieldTypes()));
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
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        MysqlUpsertStreamTableSink mysqlTableSink = new MysqlUpsertStreamTableSink(props);
        mysqlTableSink.fieldNames = fieldNames;
        mysqlTableSink.fieldTypes = fieldTypes;
        return mysqlTableSink ;
    }
}
