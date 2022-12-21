package com.cs425.iDunno;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.cs425.Messages.IDunnoMessage;
import com.cs425.Messages.IDunnoMessage.Destination;
import com.cs425.Messages.IDunnoMessage.MessageType;
import com.cs425.iDunno.Job.JobStats;
import com.cs425.fileSystem.Coordinator;
import com.cs425.fileSystem.FileServer;
import com.cs425.membership.MembershipList.MemberList;
import com.cs425.membership.MembershipList.MemberListEntry;

public class IDunnoCoordinator {
    public static Logger logger = Logger.getLogger("Coordinator");
    
    public enum Type {
        Original,
        Backup
    }

    private static final long PROTOCOL_PERIOD = 10000;

    private Type type;
    private volatile MemberList memberList;
    private volatile Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage;
    private FileServer fileServer;

    private Thread protocolThread;
    private AtomicBoolean end;

    // Map from model name to controlling job object
    private Map<String, Job> jobs;

    public IDunnoCoordinator(Type type, MemberList memberList, Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage, FileServer fileServer) {
        this.type = type;
        this.memberList = memberList;
        if (fileStorage != null) {
            this.fileStorage = Collections.synchronizedMap(fileStorage);
        }
        this.end = new AtomicBoolean();
        this.fileServer = fileServer;

        this.jobs = Collections.synchronizedMap(new HashMap<>());
    }

    // Used by backup coordinator when original coordinator fails
    public void setFileStore(Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage) {
        this.fileStorage = Collections.synchronizedMap(fileStorage);
    }

    // Handle messages to IDunno coordinator
    // Note: Messages are handled in their own thread, see Member.TCPListener()
    // Opening/closing resources is also handled there
    public void processIDunnoMessage(IDunnoMessage message, Socket client, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            switch (message.getMessageType()) {
                /* Client messages */
                case AddJob:
                    addJob(message.getModelName());
                    outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    break;

                case TrainJobs:
                    if (loadModels()) {
                        outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    } else {
                        outputStream.writeObject(new IDunnoMessage(MessageType.Fail, memberList.getSelfEntry(), Destination.Return));
                    }
                    break;

                case StartJob:
                    startJob(message.getModelName());
                    break;

                case StopJob:
                    stopJob(message.getModelName());
                    outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    break;

                case SetBatchSize:
                    setBatchSize(message.getModelName(), message.getBatchSize());
                    outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    break;

                case RequestAssignedVMs:
                    IDunnoMessage assignedVMResponse = new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return);
                    assignedVMResponse.setAssignedVMs(getAssignedVMs());
                    outputStream.writeObject(assignedVMResponse);
                    break;

                case RequestJobStats:
                    IDunnoMessage jobStatResponse = new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return);
                    jobStatResponse.setJobStats(getJobStats());
                    outputStream.writeObject(jobStatResponse);
                    break;

                /* Backup Coordinator message */
                case SendState:
                    updateState(message.getJobs(), message.getFileStore());
                    outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    break;

                /* Worker messages */
                case PollQuery:
                    outputStream.writeObject(
                        new IDunnoMessage(
                            MessageType.SendQuery,
                            memberList.getSelfEntry(),
                            Destination.Return,
                            pollQuery(message.getSender())
                        )
                    );
                    break;

                case CompletedQuery:
                    completeQuery(message.getQuery(), message.getSender());
                    outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    break;

                default:
                    assert false : "Unexpected message type received in IDunnoCoordinator";
                    break;
            }
            outputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HashMap<String, JobStats> getJobStats() {
        HashMap<String, JobStats> jobStats = new HashMap<>();
        for (Entry<String, Job> entry: jobs.entrySet()) {
            jobStats.put(entry.getKey(), entry.getValue().getStats());
        }
        return jobStats;
    }

    private HashMap<String, List<MemberListEntry>> getAssignedVMs() {
        HashMap<String, List<MemberListEntry>> assignedVMs = new HashMap<>();
        for (Entry<String, Job> entry: jobs.entrySet()) {
            assignedVMs.put(entry.getKey(), entry.getValue().getAssignedVMs());
        }
        return assignedVMs;
    }

    private void setBatchSize(String modelName, int batchSize) {
        jobs.get(modelName).setBatchSize(batchSize);
    }

    // For use by backup coordinator only
    private void updateState(HashMap<String, Job> newJobs, Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStore) {
        assert this.type == Type.Backup : "Original coordinator attempted to illegally update state";

        for (Job job: newJobs.values()) {
            job.cleanState();
        }
        this.jobs = newJobs;

        this.fileStorage = Collections.synchronizedMap(fileStore);
    }

    public Map<String, TreeMap<Integer, Set<MemberListEntry>>> getFileStore() {
        return fileStorage;
    }

    private void addJob(String modelName) {
        //Fetch sdfs file names which contain test data
        List<String> files;
        Job modelJob;
        synchronized (fileStorage) {
            files = fileStorage.keySet()
                    .stream()
                    .filter(s -> s.contains("images"))
                    .collect(Collectors.toList());
            modelJob = new Job(modelName, files);
        }

        try {
            modelJob.initializeTempFilePath();
        } catch (IOException e) {
            logger.warning("Model temp result path not initialised");
        }

        // Add the job to coordinator
        jobs.put(modelName, modelJob);
        logger.info(modelName + " job added with " + files.size() + " files to run inference on");
    }

    private void stopJob(String modelName) {
        // Set job inactive and rebalance
        jobs.get(modelName).stopJob();
        rebalance();
    }

    // Poll a query for the worker VM
    private Query pollQuery(MemberListEntry entry) {
        for(Job job: jobs.values()) {
            if (job.isActive() && job.vmAssigned(entry) && job.hasQueriesToSchedule()) {
                return job.pollQuery(entry);
            }
        }

        // VM isn't currently assigned to a job or assigned job had no more queries
        // Look at any active job for a pending query
        for(Job job: jobs.values()) {
            if (job.isActive() && job.hasQueriesToSchedule()) {
                return job.pollQuery(entry);
            }
        }

        return null;
    }

    private boolean anyJobsActive() {
        for (Job job: jobs.values()) {
            if (job.isActive()) {
                return true;
            }
        }
        return false;
    }

    private void completeQuery(Query completed, MemberListEntry worker) {
        Job modelJob = jobs.get(completed.getModelName());
        modelJob.addCompletedQuery(completed, worker);
        try {
            FileWriter resultWriter = new FileWriter(modelJob.getTemporaryFilePath(), true);
            for(String result: completed.getClassificationResults()){
                resultWriter.write(result);
                resultWriter.write('\n');
            }
            resultWriter.close();
        } catch (IOException e) {
            logger.severe("Completed query failed to record: " + e.toString());
        }

    }

    // Returns true if models were loaded at all active VMs
    public boolean loadModels() throws IOException {
        // Inform all Worker VMs to load their models
        IDunnoMessage load = new IDunnoMessage(MessageType.TrainJobs, memberList.getSelfEntry(), Destination.Worker);
        boolean success = true;
        for (MemberListEntry entry: memberList) {
            // Send message, receive response
            try {
                IDunnoMessage response = IDunnoMessage.sendMessageWithResponse(
                    load,
                    entry
                );

                // Update success value but keep going in case the rest succeed
                if (response.getMessageType() == MessageType.Fail) {
                    success = false;
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return success;
    }

    /**
     * Start protocol thread
     */
    public void startProtocol() {
        protocolThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!end.get()) {
                    // Run protocol
                    IDunnoCoordinator.this.protocol();

                    // Sleep for protocol period
                    try {
                        Thread.sleep(PROTOCOL_PERIOD);
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
            }
        });
        protocolThread.setDaemon(true);
        protocolThread.start();
    }

    public void startJob(String modelName) {
        Job job = jobs.get(modelName);
        // Ensure job not already started or completed, then call startJob
        if (!job.isActive() && !job.isCompleted()) {
            job.startJob();
        }

        // Assign job VMs
        rebalance();

        // Inform all worker threads to begin processing queries
        IDunnoMessage begin = new IDunnoMessage(MessageType.StartJob, memberList.getSelfEntry(), Destination.Worker);
        for (MemberListEntry entry: memberList) {
            // Send message, receive response
            try {
                IDunnoMessage.sendMessage(
                    begin,
                    entry
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Protocol to periodically write results and send state to standby coordinator. See startProtocol()
    private void protocol() {
        // Rebalance VMs between active jobs if necessary
        rebalance();

        // Log completed query results from active jobs to SDFS, mark all results written if job completed
        for(Map.Entry<String,Job> entry: jobs.entrySet()){
            Job currentJob = entry.getValue();
            if(currentJob.isActive()) {
                if (currentJob.isCompleted()) {
                    currentJob.setAllResultsWritten();
                }
                fileServer.processFileCommands(currentJob.getTemporaryFilePath(), currentJob.getSdfsFileName(), "put", null);
            }
        }

        // Send state to standby coordinator
        if(memberList.getBackupCoordinator() != null) {
            try {
                IDunnoMessage.sendMessageWithResponse(
                        new IDunnoMessage(
                                MessageType.SendState,
                                memberList.getSelfEntry(),
                                Destination.Coordinator,
                                new HashMap<>(this.jobs),
                                Coordinator.synchronizedDeepFileStorageCopy(fileStorage)
                        ),
                        memberList.getBackupCoordinator()
                );
            } catch (Exception e) {
                logger.warning("Couldn't reach backup. Will attempt again next protocol cycle: " + e.getMessage());
            }
        }
    }

    // Inform all jobs of VM failure
    public void onVMFailure(MemberListEntry entry) {
        for (Job job: jobs.values()) {
            job.onVMFailure(entry);
        }
    }

    // Reassign VM resources based on query rates
    private void rebalance() {
        List<Job> activeJobs = new ArrayList<>();
        for (Job job: jobs.values()) {
            if (job.isActive() && job.hasQueriesToSchedule()) {
                activeJobs.add(job);
            }
        }

        // If no active jobs, no VMs need to be assigned
        if (activeJobs.isEmpty()) {
            return;
        }

        // If one active job, assign all VMs
        if (activeJobs.size() == 1) {
            activeJobs.get(0).assignVMs(memberList.getMemberList());
            return;
        }

        assert activeJobs.size() == 2 : "Unexpected number of active jobs (>2)";

        List<List<MemberListEntry>> splitVMs;

        // Corner case where a job has no VMs assigned, initially assign both half the vms
        if (activeJobs.get(0).getAssignedVMs() == null || activeJobs.get(1).getAssignedVMs() == null) {
            int halfVMs = memberList.size() / 2;
            splitVMs = splitListIntoAssignments(halfVMs, memberList.size() - halfVMs);
        } else {
            int vms1 = activeJobs.get(0).getAssignedVMs().size();
            int vms2 = activeJobs.get(1).getAssignedVMs().size();

            double rate1 = activeJobs.get(0).getQueryRate();
            double rate2 = activeJobs.get(1).getQueryRate();

            // No rebalancing needed if rates are within 20%
            if (ratesWithinThreshhold(rate1, rate2)) {
                return;
            }

            double ratePerVM1 = rate1 / vms1;
            double ratePerVM2 = rate2 / vms2;

            // Calculate num vms assigned to first active job
            int newVMs1 = (int) (ratePerVM2 / (ratePerVM2 + ratePerVM1) * memberList.size());

            // Ensure smallest share is 1 VM
            if (newVMs1 == 0) {
                newVMs1 += 1;
            } else if(newVMs1 == memberList.size()) {
                newVMs1 -= 1;
            }

            splitVMs = splitListIntoAssignments(newVMs1, memberList.size() - newVMs1);

        }

        activeJobs.get(0).assignVMs(splitVMs.get(0));
        activeJobs.get(1).assignVMs(splitVMs.get(1));
    }

    private boolean ratesWithinThreshhold(double rate1, double rate2) {
        double percentDiff = 100 * Math.abs(rate1 - rate2) / ((rate1 + rate2) / 2.0);
        return  percentDiff < 20.0;
    }

    private List<List<MemberListEntry>> splitListIntoAssignments(int vms1, int vms2) {
        List<MemberListEntry> activeVMs = memberList.getMemberList();
        List<List<MemberListEntry>> partition = new ArrayList<>();

        partition.add(new ArrayList<MemberListEntry>(activeVMs.subList(0, vms1)));
        partition.add(new ArrayList<MemberListEntry>(activeVMs.subList(vms1, vms1 + vms2)));

        return partition;
    }
}
