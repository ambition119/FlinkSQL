package ambition.client.job;

/**
 * @Author: wpl
 */
public enum TableSinkType {
    UPSERT("Upsert"),
    BATCH("Batch"),
    APPEND("Append");

    private String sinkType;

    TableSinkType(String sinkType){
        this.sinkType = sinkType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }
}
