package ambition.client.job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.fs.Path;

/**
 * @Author: wpl
 */
public class CompilationResult implements Serializable {
    private String name ;
    private String queue;
    private int numSlots ;
    private int numContainers ;
    private int memorysize;
    private JobGraph jobGraph;
    private Throwable remoteThrowable;
    private List<Path> additionalJars;

    public CompilationResult() {
    }

    public CompilationResult(String name, String queue, int numSlots, int numContainers, int memorysize, JobGraph jobGraph, Throwable remoteThrowable, List<Path> additionalJars) {
        this.name = name;
        this.queue = queue;
        this.numSlots = numSlots;
        this.numContainers = numContainers;
        this.memorysize = memorysize;
        this.jobGraph = jobGraph;
        this.remoteThrowable = remoteThrowable;
        this.additionalJars = additionalJars;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public void setNumSlots(int numSlots) {
        this.numSlots = numSlots;
    }

    public int getNumContainers() {
        return numContainers;
    }

    public void setNumContainers(int numContainers) {
        this.numContainers = numContainers;
    }

    public int getMemorysize() {
        return memorysize;
    }

    public void setMemorysize(int memorysize) {
        this.memorysize = memorysize;
    }

    public JobGraph getJobGraph() {
        return jobGraph;
    }

    public void setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
    }

    public Throwable getRemoteThrowable() {
        return remoteThrowable;
    }

    public void setRemoteThrowable(Throwable remoteThrowable) {
        this.remoteThrowable = remoteThrowable;
    }

    public List<Path> getAdditionalJars() {
        return additionalJars;
    }

    public void setAdditionalJars(List<Path> additionalJars) {
        this.additionalJars = additionalJars;
    }

    byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(this);
        } catch (IOException e) {
            return null;
        }
        return bos.toByteArray();
    }

}