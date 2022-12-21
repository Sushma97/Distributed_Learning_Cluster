package com.cs425.iDunno;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class Query implements Serializable, Comparable<Query> {
    // Setup parameters
    private int queryId;
    private String modelName;
    private List<String> sdfsFiles;

    public void setClassificationLabels(List<String> classificationLabels) {
        this.classificationLabels = classificationLabels;
    }

    // Result classification
    private List<String> classificationLabels = null;

    // Logging for stats
    private long startTime;
    private long completionTime;
    private long coordinatorReceptionTime;

    public Query(int queryId, String modelName, List<String> sdfsFiles) {
        this.queryId = queryId;
        this.modelName = modelName;
        this.sdfsFiles = sdfsFiles;
    }

    // Parameters
    public long getQueryId() {
        return queryId;
    }

    public String getModelName() {
        return modelName;
    }

    public List<String> getQueryFiles() {
        return sdfsFiles;
    }

    // Results
    public List<String> getClassificationResults() {
        return classificationLabels;
    }

    // Timing
    public long getStartTime() {
        return startTime;
    }

    public void logStartTime() {
        this.startTime = java.lang.System.currentTimeMillis();
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public void logCompletionTime() {
        this.completionTime = java.lang.System.currentTimeMillis();
    }

    public long getCoordinatorReceptionTime() {
        return coordinatorReceptionTime;
    }

    public void logCoordinatorReceptionTime() {
        this.coordinatorReceptionTime = java.lang.System.currentTimeMillis();
    }

    public long getDuration() {
        return (completionTime - startTime)/1000;
    }

    @Override
    public boolean equals(Object otherObj) {
        if (!(otherObj instanceof Query)) {
            return false;
        }

        Query other = (Query) otherObj;
        return this.queryId == other.queryId;
    }

    @Override
    public int hashCode() {
        return queryId;
    }

    @Override
    public int compareTo(Query other) {
        return this.queryId - other.queryId;
    }

    @Override
    public String toString() {
        return "Id: " + queryId + " \tFiles: " + sdfsFiles;
    }
    
}
