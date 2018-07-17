package ambition.flink.job.sink.mysql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import ambition.flink.job.JobConstant;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @Author: wpl
 */
public class MysqlSink extends RichSinkFunction<Row>  {
    private final Properties props;
    private Connection connection;
    private CopyOnWriteArrayList<Row> rowList = new CopyOnWriteArrayList<Row>();
    private String drivername = "com.mysql.jdbc.Driver";
    private String dburl;
    private String username;
    private String password;
    private String tableName;
    private Integer batchSize = 10;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    private String insertSQL;
    public MysqlSink(Properties props, String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        this.props = props;

        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }
    private void initSql() {
        StringBuffer keyStr = new StringBuffer();
        StringBuffer valStr = new StringBuffer();
        for (String fieldName : fieldNames) {
            keyStr.append(fieldName + ",");
            valStr.append("?,");
        }
        String key = keyStr.substring(0, keyStr.length() - 1);
        String val = valStr.substring(0, valStr.length() - 1);
        insertSQL = String.format(" insert into " + tableName + "(%s) values (%s)", key, val);
    }
    private void addRowSingle(Row row) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
        makePreparedStatement(row, preparedStatement);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
    private boolean addRowBatch() {
        boolean result = false;
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
            for (Row row : rowList){
                makePreparedStatement(row, preparedStatement);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            result = true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
    private void makePreparedStatement(Row row, PreparedStatement preparedStatement) throws SQLException {
        int count = 0;
        for (Object type : fieldTypes) {
            switch (type.toString().toLowerCase()) {
                case "string":
                    preparedStatement.setString(count + 1, String.valueOf(row.getField(count)));
                    break;
                case "boolean":
                    preparedStatement.setBoolean(count + 1, (Boolean) row.getField(count));
                    break;
                case "byte":
                    preparedStatement.setByte(count + 1, (Byte) row.getField(count));
                    break;
                case "short":
                    preparedStatement.setShort(count + 1, (Short) row.getField(count));
                    break;
                case "integer":
                    preparedStatement.setInt(count + 1, (Integer) row.getField(count));
                    break;
                case "long":
                    preparedStatement.setLong(count + 1, (Long) row.getField(count));
                    break;
                case "float":
                    preparedStatement.setFloat(count + 1, (Float) row.getField(count));
                    break;
                case "double":
                    preparedStatement.setDouble(count + 1, (Double) row.getField(count));
                    break;
                case "bigdecimal":
                    preparedStatement.setBigDecimal(count + 1, (BigDecimal) row.getField(count));
                    break;
                case "date":
                    preparedStatement.setDate(count + 1, (Date) row.getField(count));
                    break;
                case "time":
                    preparedStatement.setTime(count + 1, (Time) row.getField(count));
                    break;
                case "timestamp":
                    preparedStatement.setTimestamp(count + 1, (Timestamp) row.getField(count));
                    break;
                default:
                    break;
            }
            count++;
        }
    }
    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
    @Override
    public void open(Configuration parameters) throws Exception {

        String conn = props.getProperty(JobConstant.CONNECTION);
        String dbName = props.getProperty(JobConstant.DBNAME);
        dburl = "jdbc:mysql://" + conn + "/" + dbName + "?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&autoReconnect=true&autoReconnectForPools=true";
        username = props.getProperty(JobConstant.USER);
        password = props.getProperty(JobConstant.PASS);
        tableName = props.getProperty(JobConstant.TABLENAME);
        batchSize = Integer.valueOf(props.getOrDefault(JobConstant.BATCHSIZE, batchSize).toString());

        initSql();

        if (connection == null) {
            Class.forName(drivername);
            connection = DriverManager.getConnection(dburl, username, password);
        }

    }
    @Override
    public void invoke(Row row) throws Exception {
        if (batchSize > 0) {
            if (rowList == null) {
                rowList = new CopyOnWriteArrayList<Row>();
            }
            rowList.add(row);
            if (rowList.size() >= batchSize){
                boolean result = addRowBatch();
                if (result){
                    rowList.clear();
                }
            }
        } else {
            addRowSingle(row);
        }
    }
}
