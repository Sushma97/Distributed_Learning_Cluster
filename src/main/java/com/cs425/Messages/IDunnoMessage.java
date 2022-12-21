package com.cs425.Messages;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.cs425.iDunno.Job;
import com.cs425.iDunno.Query;
import com.cs425.iDunno.Job.JobStats;
import com.cs425.membership.MembershipList.MemberListEntry;

public class IDunnoMessage implements Serializable {

    // Fire and forget
    public static void sendMessage(IDunnoMessage message, MemberListEntry entry) throws UnknownHostException, IOException {
        // Open resources
        Socket socket = new Socket(entry.getHostname(), entry.getPort());
        ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

        // Write message
        output.writeObject(message);
        output.flush();

        // Close resources
        inputStream.close();
        output.close();
        socket.close();
    }

    // Receive response
    public static IDunnoMessage sendMessageWithResponse(IDunnoMessage message, MemberListEntry entry) throws UnknownHostException, IOException, ClassNotFoundException {
        IDunnoMessage response = null;

        // Open resources
        Socket socket = new Socket(entry.getHostname(), entry.getPort());
        ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

        // Write message
        output.writeObject(message);
        output.flush();

        // Recieve response
        response = (IDunnoMessage) inputStream.readObject();

        // Close resources
        inputStream.close();
        output.close();
        socket.close();

        return response;
    }

    public enum MessageType {
        // Responses
        Ok,
        Fail,

        // Client commands
        AddJob,
        TrainJobs,
        StartJob,
        StopJob,
        SetBatchSize,
        RequestJobStats,
        RequestAssignedVMs,
        
        // Backup Coordinator
        SendState,

        // Worker-Coordinator communication
        PollQuery,
        SendQuery,
        CompletedQuery,
    }

    public enum Destination {
        Coordinator,
        Worker,
        Return
    }

    // Message fields common to all
    private MessageType messageType;
    private MemberListEntry sender;
    private Destination destination;

    // Job message fields
    private String modelName;

    // State fields
    HashMap<String, Job> jobs;
    Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage;

    // Query message fields
    private Query query;

    // Batch size message
    private int batchSize;

    // List of stats for each job tracked by coordinator
    private HashMap<String, JobStats> jobStats;

    // List of assigned VMs to each model
    private HashMap<String, List<MemberListEntry>> assignedVMs;

    // Generic message constructor
    public IDunnoMessage(MessageType messageType, MemberListEntry sender,
            Destination destination) {
        this.messageType = messageType;
        this.sender = sender;
        this.destination = destination;
    }

    // separate to avoid type erasure conflicts for constructors
    public void setJobStats(HashMap<String, JobStats> jobStats) {
        this.jobStats = jobStats;
    }
    public Map<String, JobStats> getJobStats() {
        return jobStats;
    }

    public void setAssignedVMs(HashMap<String, List<MemberListEntry>> assignedVMs) {
        this.assignedVMs = assignedVMs;
    }
    public HashMap<String, List<MemberListEntry>> getAssignedVMs() {
        return assignedVMs;
    }

    // Message constructor with state
    public IDunnoMessage(MessageType messageType, MemberListEntry sender, Destination destination,
            HashMap<String, Job> jobs, Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage) {
        this.messageType = messageType;
        this.sender = sender;
        this.destination = destination;
        this.jobs = jobs;
        this.fileStorage = fileStorage;
    }

    // Message constructor with model name
    public IDunnoMessage(MessageType messageType, MemberListEntry sender,
            Destination destination, String modelName) {
        this.messageType = messageType;
        this.sender = sender;
        this.destination = destination;
        this.modelName = modelName;
    }

    // Message constructor with query
    public IDunnoMessage(MessageType messageType, MemberListEntry sender,
            Destination destination, Query query) {
        this.messageType = messageType;
        this.sender = sender;
        this.destination = destination;
        this.query = query;
    }

    // Message constructor with batch size
    public IDunnoMessage(MessageType messageType, MemberListEntry sender,
            Destination destination, String modelName, int batchSize) {
        this.messageType = messageType;
        this.sender = sender;
        this.destination = destination;
        this.modelName = modelName;
        this.batchSize = batchSize;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public MemberListEntry getSender() {
        return sender;
    }

    public Destination getDestination() {
        return destination;
    }

    public HashMap<String, Job> getJobs() {
        return jobs;
    }

    public Map<String, TreeMap<Integer, Set<MemberListEntry>>> getFileStore() {
        return fileStorage;
    }

    public Query getQuery() {
        return query;
    }

    public String getModelName() {
        return modelName;
    }

    public int getBatchSize() {
        return batchSize;
    }
    
}
