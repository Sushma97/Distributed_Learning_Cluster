package com.cs425.iDunno;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.cs425.membership.MembershipList.MemberListEntry;


// For keeping track of active, standby and recently completed jobs, as well as allocation of resources
public class Job implements Serializable {
    // parameters
    private String modelName;
    List<MemberListEntry> assignedVMs;
    private int batchSize = 1;
    List<String> inferenceSdfsFiles;
    private String temporaryFilePath;
    private String sdfsFileName;

    public String getTemporaryFilePath() {
        return temporaryFilePath;
    }

    public String getSdfsFileName() {
        return sdfsFileName;
    }

    public void initializeTempFilePath() throws IOException {
            temporaryFilePath = constants.RESULT_BASE_PATH + modelName + "_result.txt";
            sdfsFileName = modelName + "_result.txt";
            Files.deleteIfExists(Paths.get(temporaryFilePath));
            new File(temporaryFilePath).createNewFile();
    }

    // remainingQueries is functionally a list of all queries that have not yet been completed, including those in progress
    // This is needed to send state to the standby coordinator
    private List<Query> remainingQueries;

    // completedQueries is all queries that have been finished since the beginning of execution
    // This is needed to send state to the standby coordinator
    private List<Query> completedQueries;


    // querySchedule is remaining queries that have NOT been processed. Does not include in-progress queries
    // NOT to be sent to standby coordinator, updated in cleanState() instead
    private Queue<Query> querySchedule;

    // To facilitate replacing failures
    private Map<MemberListEntry, Query> queriesInProgress;

    // Queries completed in the last 10 seconds, for 
    private Set<Query> queriesFromLastTenSeconds;

    // True iff job has been started
    private boolean active = false;

    // True iff job is completed and results have been written to SDFS
    private boolean allResultsWritten = false;

    // Constructor
    public Job(String modelName, List<String> inferenceSdfsFiles) {
        this.modelName = modelName;
        this.inferenceSdfsFiles = inferenceSdfsFiles;

        // Instantiate data structures
        this.completedQueries = Collections.synchronizedList(new ArrayList<>());
        this.queriesInProgress = Collections.synchronizedMap(new HashMap<>());
        this.queriesFromLastTenSeconds = Collections.synchronizedSet(new HashSet<>());
    }

    /**
     * Cleans up the state of the job to reflect all remaining queries (including previously in-progress ones so there is no data loss)
     * @implNote For use by backup coordinator only
     * @see IDunnoCoordinator
     */
    public void cleanState() {
        if (remainingQueries != null) {
            querySchedule = new ConcurrentLinkedQueue<>(remainingQueries);
        }

        if (queriesInProgress != null) {
            queriesInProgress.clear();
        }

        if (queriesFromLastTenSeconds != null) {
            queriesFromLastTenSeconds.clear();
        }
    }

    // Set batch size
    public synchronized void setBatchSize(int newBatchSize) {
        batchSize = newBatchSize;
    }

    // Gets the next query not already in progress
    public Query pollQuery(MemberListEntry worker) {
        if (querySchedule.isEmpty()) {
            return null;
        }

        Query nextQuery = querySchedule.poll();
        queriesInProgress.put(worker, nextQuery);

        return nextQuery;
    }

    public boolean hasQueriesToSchedule() {
        return !querySchedule.isEmpty();
    }

    // Marks a query completed
    public void addCompletedQuery(Query completedQuery, MemberListEntry worker) {
        completedQuery.logCoordinatorReceptionTime();
        queriesInProgress.remove(worker);

        // Ensure no duplicate completions (by backup)
        if (!completedQueries.contains(completedQuery)) {
            completedQueries.add(completedQuery);
        }

        addToLastTenSecondsQueries(completedQuery);

        remainingQueries.remove(completedQuery);

        if (remainingQueries.isEmpty()) {
            assignedVMs = null;
        }
    }

    // Adds a newly completed query to the set of queries completed in the last 10 seconds
    private void addToLastTenSecondsQueries(Query query) {
        // Remove up to 10 secs before current time to satisfy window
        queriesFromLastTenSeconds.removeIf(q -> (query.getCoordinatorReceptionTime() - q.getCoordinatorReceptionTime() > 10000));

        queriesFromLastTenSeconds.add(query);
    }

    // Reschedule in-progress query from failed node
    public void onVMFailure(MemberListEntry worker) {
        if (queriesInProgress.get(worker) != null) {
            querySchedule.add(queriesInProgress.get(worker));
        }
        queriesInProgress.remove(worker);
        if (assignedVMs != null && assignedVMs.contains(worker)) {
            assignedVMs.remove(worker);
        }
    }

    // VM assignment
    public void assignVMs(List<MemberListEntry> vmsToAssign) {
        assignedVMs = Collections.synchronizedList(vmsToAssign);
    }

    public boolean vmAssigned(MemberListEntry entry) {
        if (assignedVMs == null) {
            return false;
        }
        return assignedVMs.contains(entry);
    }

    public List<MemberListEntry> getAssignedVMs() {
        return assignedVMs;
    }

    // Get completed queries to log results
    public synchronized void startJob() {
        populateQueries();
        active = true;
    }

    public synchronized void stopJob() {
        active = false;
    }

    // Add new queries from list of sdfs files to run inference on
    private void populateQueries() {
        // Split list into lists of size batchSize
        remainingQueries = new ArrayList<>();

        List<List<String>> batchedInferenceFiles = ListUtils.partition(inferenceSdfsFiles, batchSize);

        int id = 0;
        for (List<String> inferenceFileBatch : batchedInferenceFiles) {
            Query query = new Query(id, this.modelName, new ArrayList<>(inferenceFileBatch));
            remainingQueries.add(query);
            id++;
        }
        
        // add queries to queue schedule
        querySchedule = new ConcurrentLinkedQueue<>(remainingQueries);
    }

    /**
     * Active status of the job
     * @return true if job is processing queries
     */
    public boolean isActive() {
        return active;
    }

    public List<Query> getCompletedQueries() {
        return completedQueries;
    }

    public boolean isCompleted() {
        return remainingQueries != null && remainingQueries.isEmpty();
    }

    public void setAllResultsWritten() {
        allResultsWritten = true;
        active = false;
    }

    public boolean getAllResultsWritten() {
        return allResultsWritten;
    }

    /* STATS */

    /**
     * Get stats about job query processing
     * @return Stats containing moving window average query rate for the last 10 seconds,
     * total number of queries processed, and descriptive statistics (avg, stdev, percentiles).
     */
    public JobStats getStats() {
        DescriptiveStatistics ds = new DescriptiveStatistics(completedQueries.stream().mapToDouble(Query::getDuration).toArray());
        return new JobStats(
            getNumCompletedQueries(),
            getQueryRate(),
            ds.getMean(),
            ds.getStandardDeviation(),
            ds.getPercentile(50),
            ds.getPercentile(90),
            ds.getPercentile(95),
            ds.getPercentile(99)
        );
    }

    // Returns queries per second of this job
    public double getQueryRate() {
        return queriesFromLastTenSeconds.size() / 10.0;
    }

    // Gets lifetime completed query batches
    private int getNumCompletedQueries() {
        return completedQueries.size();
    }

    public static class JobStats implements Serializable {
        int totalCompletedQueries;
        double movingWindowQueryRate;
        double averageProcessingTime;
        double standardDeviation;
        double median;
        double percentile90;
        double percentile95;
        double percentile99;
        
        public JobStats(int totalCompletedQueries, double movingWindowQueryRate, double averageProcessingTime,
                double standardDeviation, double median, double percentile90, double percentile95,
                double percentile99) {
            this.totalCompletedQueries = totalCompletedQueries;
            this.movingWindowQueryRate = movingWindowQueryRate;
            this.averageProcessingTime = averageProcessingTime;
            this.standardDeviation = standardDeviation;
            this.median = median;
            this.percentile90 = percentile90;
            this.percentile95 = percentile95;
            this.percentile99 = percentile99;
        }

        public int getTotalCompletedQueries() {
            return totalCompletedQueries;
        }

        public double getMovingWindowQueryRate() {
            return movingWindowQueryRate;
        }

        public double getAverageProcessingTime() {
            return averageProcessingTime;
        }

        public double getStandardDeviation() {
            return standardDeviation;
        }

        public double getMedian() {
            return median;
        }

        public double get90thPercentile() {
            return percentile90;
        }

        public double get95thPercentile() {
            return percentile95;
        }

        public double get99thPercentile() {
            return percentile99;
        }

        @Override
        public String toString() {
            return "JobStats{" +
                    "totalCompletedQueries=" + totalCompletedQueries +
                    ", movingWindowQueryRate=" + movingWindowQueryRate +
                    ", averageProcessingTime=" + averageProcessingTime +
                    ", standardDeviation=" + standardDeviation +
                    ", median=" + median +
                    ", percentile90=" + percentile90 +
                    ", percentile95=" + percentile95 +
                    ", percentile99=" + percentile99 +
                    '}';
        }
    }
}
