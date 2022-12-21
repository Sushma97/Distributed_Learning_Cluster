package com.cs425.membership;

import com.cs425.Messages.FileMessage;
import com.cs425.Messages.IDunnoMessage;
import com.cs425.Messages.MembershipMessage;
import com.cs425.Messages.MembershipMessage.MessageType;
import com.cs425.fileSystem.Coordinator;
import com.cs425.fileSystem.FileServer;
import com.cs425.iDunno.IDunnoCoordinator;
import com.cs425.iDunno.Job;
import com.cs425.iDunno.Worker;
import com.cs425.iDunno.IDunnoCoordinator.Type;
import com.cs425.iDunno.constants;
import com.cs425.membership.MembershipList.MemberList;
import com.cs425.membership.MembershipList.MemberListEntry;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Member {

    // Member & introducer info
    private String host;
    private int port;
    private Date timestamp;
    public String introducerHost;
    public int introducerPort;

    // Protocol settings
    private static final long PROTOCOL_TIME = 4000;
    private static final int NUM_MONITORS = 2;

    // Sockets
    private ServerSocket server;
    private DatagramSocket socket;

    // Membership list and owner entry
    public volatile MemberList memberList;
    public MemberListEntry selfEntry;

    // Threading resources
    private Thread mainProtocolThread;
    private Thread TCPListenerThread;
    private FileServer fileServer;

    private AtomicBoolean joined;
    private AtomicBoolean end;

    // Reference to coordinator if this node elected
    private Coordinator coordinator = null;

    // IDunno Coordinator
    private IDunnoCoordinator iDunnoCoordinator = null;

    // IDunno Worker thread
    private Worker worker;

    // Logger
    public static Logger logger = Logger.getLogger("MemberLogger");

    // Constructor and beginning of functionality
    public Member(String host, int port, String introducerHost, int introducerPort) throws SecurityException, IOException {
        assert(host != null);
        assert(timestamp != null);

        this.host = host;
        this.port = port;

        this.introducerHost = introducerHost;
        this.introducerPort = introducerPort;

        joined = new AtomicBoolean();
        end = new AtomicBoolean();

        Handler fh = new FileHandler("/srv/mp2_logs/member.log");
//        Handler fh = new FileHandler("./member.log");
        fh.setFormatter(new SimpleFormatter());
        logger.setUseParentHandlers(false);
        logger.addHandler(fh);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    // Process command line inputs
    public void start() throws ClassNotFoundException, InterruptedException {
        logger.info("Member process started");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("MemberProcess$ ");
                String[] input = stdin.readLine().split(" ");
                String command = input[0];
                System.out.println();
                long startTime = System.currentTimeMillis();
                switch (command) {
                    case "join":
                        joinGroup();
                        break;
                
                    case "leave":
                        leaveGroup(true);
                        break;
                
                    case "list_mem":
                        if (joined.get()) {
                            System.out.println(memberList);
                        } else {
                            System.out.println("Not joined");
                        }
                        break;
                
                    case "list_self":
                        if (joined.get()) {
                            System.out.println(selfEntry);
                        } else {
                            System.out.println("Not joined");
                        }
                        break;
                    case "store":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(null, null, input[0], null);
                        break;
                    case "put":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[1], input[2].replace('/','#'), input[0], 0);
                        break;
                    case "get":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[2], input[1].replace('/','#'), input[0], 0);
                        break;
                    case "delete":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands("", input[1].replace('/','#'), input[0], 0);
                        break;
                    case "ls":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands("", input[1].replace('/','#'), input[0], 0);
                        break;
                    case "get-versions":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[3], input[1].replace('/','#'), input[0], Integer.parseInt(input[2]));
                        break;
                    case "train-start":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        IDunnoMessage response = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.TrainJobs,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator
                        ), memberList.getCoordinator());
                        if (response.getMessageType() == IDunnoMessage.MessageType.Fail){
                            System.out.println("Failed to train the model at all VMs. Try again");
                            break;
                        }
                        System.out.println("Finished training successfully");
                        break;
                    case "add-job":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        if (!constants.models.contains(input[1])){
                            System.out.println("model name not in system");
                            break;
                        }
                        IDunnoMessage jobResponse = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.AddJob,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator,
                                input[1]
                        ), memberList.getCoordinator());
                        if (jobResponse.getMessageType() == IDunnoMessage.MessageType.Ok){
                            System.out.println("Successfully added the job");
                            break;
                        }
                        break;
                    case "start-job":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        if (!constants.models.contains(input[1])){
                            System.out.println("model name not in system");
                            break;
                        }
                        IDunnoMessage.sendMessage(new IDunnoMessage(
                                IDunnoMessage.MessageType.StartJob,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator,
                                input[1]
                        ), memberList.getCoordinator());
                        break;
                    case "stop-job":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        if (!constants.models.contains(input[1])){
                            System.out.println("model name not in system");
                            break;
                        }
                        IDunnoMessage stopJobResponse = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.StopJob,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator,
                                input[1]
                        ), memberList.getCoordinator());
                        if (stopJobResponse.getMessageType() == IDunnoMessage.MessageType.Ok){
                            System.out.println("Successfully stopped the job");
                            break;
                        }
                        break;
                    case "set-batch-size":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        if (!constants.models.contains(input[1])){
                            System.out.println("model name not in system");
                            break;
                        }
                        IDunnoMessage batchSizeResponse = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.SetBatchSize,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator,
                                input[1],
                                Integer.parseInt(input[2])
                        ), memberList.getCoordinator());
                        if (batchSizeResponse.getMessageType() == IDunnoMessage.MessageType.Ok){
                            System.out.println("Successfully set the batch size");
                            break;
                        }
                        break;
                    case "get-job-stats":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        IDunnoMessage jobStatsResponse = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.RequestJobStats,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator
                        ), memberList.getCoordinator());
                        if (jobStatsResponse.getMessageType() == IDunnoMessage.MessageType.Ok){
                            Map<String, Job.JobStats> jobStats = jobStatsResponse.getJobStats();
                            for(Map.Entry<String, Job.JobStats> entry: jobStats.entrySet()){
                                System.out.println("Job Stats for " + entry.getKey() + " is:");
                                System.out.println(entry.getValue().toString());
                            }
                            break;
                        }
                        break;
                    case "get-job-assigned-VMs":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        IDunnoMessage jobVM = IDunnoMessage.sendMessageWithResponse(new IDunnoMessage(
                                IDunnoMessage.MessageType.RequestAssignedVMs,
                                memberList.getSelfEntry(),
                                IDunnoMessage.Destination.Coordinator
                        ), memberList.getCoordinator());
                        if (jobVM.getMessageType() == IDunnoMessage.MessageType.Ok){
                            //TODO: Check if job is completed and then don't print.
                            Map<String, List<MemberListEntry>> jobVms = jobVM.getAssignedVMs();
                            for(Map.Entry<String, List<MemberListEntry>> entry: jobVms.entrySet()){
                                if(entry.getValue() == null || entry.getValue().isEmpty()){
                                    System.out.println("No assigned VMs for " + entry.getKey());
                                    continue;
                                }
                                System.out.println("VMs assigned for " + entry.getKey() + " is:");
                                for(MemberListEntry member: entry.getValue()){
                                    System.out.println(member.toString());
                                }
                            }
                            break;
                        }
                        break;
                    case "populate-test-data":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        List<Path> result;
                        try (Stream<Path> walk = Files.walk(Paths.get(input[1]))) {
                            result = walk.filter(Files::isRegularFile)
                                    .collect(Collectors.toList());
                        }
                        int filesPut = 0;
                        for(Path path: result) {
                            if (fileServer.processFileCommands(path.toString(), constants.SDFS_LOCAL_PATH+path.getFileName(), "put", null)) {
                                filesPut++;
                            }
                        }
                        System.out.println("Finished populating data: " + filesPut + " files written to SDFS");
                        break;
                    default:
                    System.out.println("Unrecognized command, type 'join', 'leave', 'list_mem','list_self', ' put localfilename sdfsfilename', " +
                            "' get sdfsfilename localfilename', 'delete sdfsfilename', 'ls sdfsfilename', 'store', " +
                            "'get-versions sdfsfilename num-versions localfilename', 'train-start', 'add-job modelName', 'start-job modelName', 'stop-job modelName', 'set-batch-size modelName batchSize");
                        break;
                }
                logger.info("Time taken to process the command " + command + " is " + (System.currentTimeMillis() - startTime));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
        }
    }


    private void joinGroup() throws IOException, ClassNotFoundException {
        // Do nothing if already joined
        if (joined.get()) {
            return;
        }

        logger.info("Member process received join command");

        end.set(false);

        // Initialize incarnation identity
        this.timestamp = new Date();
        this.selfEntry = new MemberListEntry(host, port, timestamp);

        logger.info("New entry created: " + selfEntry);

        // Get member already in group
        MemberListEntry groupProcess = getGroupProcess();

        boolean firstMember = false;

        if (groupProcess != null) {
            // Get member list from group member and add self
            memberList = requestMemberList(groupProcess);
            memberList.addNewOwner(selfEntry);
            logger.info("Retrieved membership list");
        } else {
            // This is the first member of the group
            logger.info("First member of group");
            memberList = new MemberList(selfEntry);

            firstMember = true;
        }

        // Create file server
        fileServer = new FileServer(host, port, memberList);
        logger.info("File server created");

        // Create Worker
        worker = new Worker(memberList, fileServer);

        // Create TCP server socket
        server = new ServerSocket(port);

        // Start server for handling TCP messages
        TCPListenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.TCPListener();
            }
        });
        TCPListenerThread.start();
        logger.info("TCP Server started");

        if (firstMember) {
            memberList.updateCoordinator(selfEntry);
            coordinator = new Coordinator(memberList, end);
            iDunnoCoordinator = new IDunnoCoordinator(Type.Original, memberList, coordinator.getFileStore(), fileServer);
            // Start coordinator protocol thread
            iDunnoCoordinator.startProtocol();
        } else if (memberList.size() == 2) {
            // If this is the 2nd to join, this is the backup coordinator
            memberList.updateBackupCoordinator(selfEntry);
            iDunnoCoordinator = new IDunnoCoordinator(Type.Backup, memberList, null, fileServer);
        }

        // Communicate join
        disseminateMessage(new MembershipMessage(selfEntry, MessageType.Join, selfEntry));
        logger.info("Process joined");

        // Start main protocol
        mainProtocolThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.mainProtocol();
            }
        });
        mainProtocolThread.setDaemon(true);
        mainProtocolThread.start();
        logger.info("Main protocol started");

        joined.set(true);
    }

    // Fetch member details of member already present in group
    private MemberListEntry getGroupProcess() throws IOException, ClassNotFoundException {
        Socket introducer = new Socket(introducerHost, introducerPort);
        logger.info("Connected to " + introducer);
        ObjectOutputStream output = new ObjectOutputStream(introducer.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(introducer.getInputStream());

        // Send self entry to introducer
        output.writeObject(selfEntry);
        output.flush();

        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(selfEntry) + " bytes over TCP to introducer");

        // receive running process
        MemberListEntry runningProcess = (MemberListEntry) input.readObject();

        // Close resources
        input.close();
        output.close();
        introducer.close();

        logger.info("Connection to introducer closed");

        return runningProcess;
    }

    // Fetch membership details from a member already in group
    private MemberList requestMemberList(MemberListEntry groupProcess) throws IOException, ClassNotFoundException {
        Socket client = new Socket(groupProcess.getHostname(), groupProcess.getPort());
        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(client.getInputStream());


        // Request membership list
        MembershipMessage message = new MembershipMessage(selfEntry, MessageType.MemberListRequest, selfEntry);

        output.writeObject(message);
        output.flush();
        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for membership list request");

        MemberList retrievedList = (MemberList) input.readObject();

        // Close resources
        input.close();
        output.close();
        client.close();

        return retrievedList;
    }

    private void leaveGroup(boolean sendMessage) throws IOException, InterruptedException {
        // Do nothing if not joined
        if (!joined.get()) {
            return;
        }

        logger.info("Leave command received");

        // Disseminate leave if necessary
        if (sendMessage) {
            disseminateMessage(new MembershipMessage(selfEntry, MessageType.Leave, selfEntry));
            logger.info("Request to leave disseminated");
        }

        // Close resources
        end.set(true);

        mainProtocolThread.join();
        logger.info("Main Protocol stopped");

        server.close();
        TCPListenerThread.join();
        logger.info("TCP server closed");

        memberList = null;
        selfEntry = null;
        coordinator = null;
        iDunnoCoordinator = null;
        
        logger.info("Process left");
        joined.set(false);
    }

    // Uses fire-and-forget paradigm
    public void disseminateMessage(MembershipMessage message) {
        synchronized (memberList) {
            for (MemberListEntry entry: memberList) {
                // Don't send a message to ourself
                if (entry.equals(selfEntry)) {
                    continue;
                }

                try {
                    // Open resources
                    Socket groupMember = new Socket(entry.getHostname(), entry.getPort());
                    ObjectOutputStream output = new ObjectOutputStream(groupMember.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(groupMember.getInputStream());

                    // Send message
                    output.writeObject(message);
                    output.flush();

                    switch (message.getMessageType()) {
                        case Join:
                            logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Leave:
                            logger.info("LEAVE: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Crash:
                            logger.info("CRASH: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        default:
                            assert(false);
                    }

                    // Close resources
                    input.close();
                    output.close();
                    groupMember.close();
                } catch (IOException e) {
                    continue;
                }
            }
        }
    }

    // Uses fire-and-forget paradigm
    public void sendMessageToSuccessor(MembershipMessage message) {
        MemberListEntry successor;

        // Successor should exist
        assert(memberList.hasSuccessor());

        successor = memberList.getSuccessors(1).get(0);

        try {
            // Open resources
            Socket successorSocket = new Socket(successor.getHostname(), successor.getPort());
            ObjectOutputStream output = new ObjectOutputStream(successorSocket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(successorSocket.getInputStream());

            // Send message
            output.writeObject(message);
            output.flush();

            logger.info("Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP to successor");

            // Close resources
            input.close();
            output.close();
            successorSocket.close();
        } catch (IOException e) {
            return;
        }
    }


    // Thread methods
    private void TCPListener() {
        while (!end.get()) {
            try {
                Socket client = server.accept();
                logger.info("TCP connection established from " + client.toString());

                // Process message in own thread to prevent race condition on membership list
                Thread processMessageThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Member.this.processTCPMessage(client);
                    }
                });
                processMessageThread.start();

            } catch(SocketException e) {
                break;
            } catch (Exception e) {
                continue;
            }
        }
        
    }

    private void processTCPMessage(Socket client) {
        try {
            // Open resources
            ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(client.getInputStream());

            // Recieve message
            Object message = input.readObject();

            // Process based on message type
            if (message instanceof MembershipMessage) {
                processMembershipMessage((MembershipMessage) message, client, input, output);
            } else if (message instanceof FileMessage) {
                processFileMessage((FileMessage) message, client, input, output);
            } else if (message instanceof IDunnoMessage) {
                processIDunnoMessage((IDunnoMessage) message, client, input, output);
            } else {
                assert false: "Unrecognized message type";
            }

            // Close resources
            input.close();
            output.close();
            client.close();
        } catch (Exception e) {
            logger.warning("Connection interrupted on message receive");
        }
    }

    // Process membership related messages message
    private void processMembershipMessage(MembershipMessage message, Socket client, ObjectInputStream input, ObjectOutputStream output) throws IOException, InterruptedException {
        // Perform appropriate action
        switch(message.getMessageType()) {
            case MemberListRequest:
                output.writeObject(memberList);
                output.flush();
                logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP containing membership list");
                break;

            case Join:
                logger.info("Received message for process joining group: " + message.getSubjectEntry());

                if (memberList.addEntry(message.getSubjectEntry())) {
                    logger.info("Process added to membership list: " + message.getSubjectEntry());
                }

                // If this was the second machine to join, it is the backup coordinator
                if (memberList.size() == 2) {
                    memberList.updateBackupCoordinator(message.getSubjectEntry());
                }

                break;

            case Leave:
                logger.info("Received message for process leaving group: " + message.getSubjectEntry());
                nodeLeave(message.getSubjectEntry());
                break;

            case Crash:
                // Ignore Crash messages from members not in group (could be faulty)
                if (!memberList.containsEntry(message.getSender())) {
                    return;
                }

                logger.warning("Message received for crashed process: " + message.getSubjectEntry());
                if (selfEntry.equals(message.getSubjectEntry())) {
                    // False crash of this node detected
                    System.out.println("\nFalse positive crash of this node detected. Stopping execution.\n");
                    logger.warning("False positive crash of this node detected. Stopping execution.");

                    // Leave group silently
                    leaveGroup(false);

                    // Command prompt
                    System.out.print("MemberProcess$ ");
                    break;
                }
                nodeLeave(message.getSubjectEntry());
                break;

            // Do nothing
            case CheckAlive:
            default:
                break;
        }
    }

    private void nodeLeave(MemberListEntry entry) {
        if (memberList.removeEntry(entry)) {
            logger.info("Process removed from membership list: " + entry);

            // If coordinator left and backup was promoted
            if (memberList.getBackupPromoted() && memberList.getCoordinator() == selfEntry && coordinator == null) {
                iDunnoCoordinator.startProtocol();
                coordinator = new Coordinator(memberList, end, iDunnoCoordinator);
                System.out.println("Backup has promoted.\n");
                System.out.print("MemberProcess$ ");
            }

            // Start backup coordinator if this node chosen
            if (memberList.getBackupCoordinator() == selfEntry && iDunnoCoordinator == null) {
                iDunnoCoordinator = new IDunnoCoordinator(Type.Backup, memberList, null, fileServer);
            }

            // Notify IDunno coordinator of worker failure to reschedule in progress query batch
            if (iDunnoCoordinator != null) {
                iDunnoCoordinator.onVMFailure(entry);
            }
        }
    }

    // Process file server related messages
    private void processFileMessage(FileMessage message, Socket client, ObjectInputStream input, ObjectOutputStream output) {
        // Send to appropriate server (Coordinator or FileServer)
        switch (message.getDestination()) {
            case FileServer:
                fileServer.processFileMessage(message, client, input, output);
                break;
            case Coordinator:
                assert coordinator != null: "Received message for coordinator while this node is not the coordinator";
                coordinator.processFileMessage(message, client, input, output);
                break;
            case Client:
                assert false: "TCP server recieved message meant for an existing connection";
            default:
                break;
        }
    }

    // Process IDunno related messages
    private void processIDunnoMessage(IDunnoMessage message, Socket client, ObjectInputStream input, ObjectOutputStream output) {
        // Send to appropriate server (Coordinator or FileServer)
        switch (message.getDestination()) {
            case Coordinator:
                assert iDunnoCoordinator != null: "Received IDuno message for coordinator while this node is not the coordinator or backup";
                iDunnoCoordinator.processIDunnoMessage(message, client, input, output);
                break;
            case Worker:
                worker.processIDunnoMessage(message, client, input, output);
                break;
            case Return:
                assert false: "TCP server recieved IDunno message meant for an existing connection";
            default:
                break;
        }
    }

    // For receiving UDP messages and responding
    // For sending pings and checking ack
    private void mainProtocol() {
        try {
            socket = new DatagramSocket(selfEntry.getPort());
            // Maintain list of acknowledgements to know which member sent the ACK
            List<AtomicBoolean> ackSignals = new ArrayList<>(NUM_MONITORS);
            for (int i = 0; i < NUM_MONITORS; i++) {
                ackSignals.add(new AtomicBoolean());
            }
            // Receive ping and send ACK
            Receiver receiver = new Receiver(socket, selfEntry, end, ackSignals);
            receiver.setDaemon(true);
            receiver.start();
            logger.info("UDP Socket opened");
            while(!end.get()) {
                List<MemberListEntry> successors;

                // Get the next successors to send ping to
                successors = memberList.getSuccessors(NUM_MONITORS);
                // Update receiver about the successor information
                receiver.updateAckers(successors);
                // Send ping
                for (int i = 0; i < successors.size(); i++) {
                    new SenderProcess(ackSignals.get(i), successors.get(i), 1500).start();
                }

                //Wait for protocol time period
                Thread.sleep(PROTOCOL_TIME);
            }
            socket.close();
            logger.info("UDP Socket closed");
            receiver.join();
        } catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Process to send Ping and wait for ACK
    private class SenderProcess extends Thread {
        public MemberListEntry member;
        private AtomicBoolean ackSignal;
        private long timeOut;

        private void ping(MemberListEntry member, MemberListEntry sender) throws IOException {
            MembershipMessage message = new MembershipMessage(selfEntry, MembershipMessage.MessageType.Ping, sender);
            UDPProcessing.sendPacket(socket, message, member.getHostname(), member.getPort());
        }

        public SenderProcess(AtomicBoolean ackSignal, MemberListEntry member, long timeOut)  {
            this.member = member;
            this.ackSignal = ackSignal;
            this.timeOut = timeOut;
        }

        @Override
        public void run() {

            try {
                // Ping successor
                synchronized (ackSignal) {
                    ackSignal.set(false);
                    logger.info("Pinging " + member);
                    ping(member, selfEntry);
                    ackSignal.wait(timeOut);
                }

                // Handle ACK timeout
                if (!ackSignal.get()) {
                    logger.warning("ACK not received from " + member);

                    // Try one last time via TCP to prevent false positives
                    try {
                        Socket tryConnection = new Socket(member.getHostname(), member.getPort());
                        ObjectOutputStream tryConnectionOutput = new ObjectOutputStream(tryConnection.getOutputStream());
                        ObjectInputStream tryConnectionInput = new ObjectInputStream(tryConnection.getInputStream());

                        tryConnectionOutput.writeObject(new MembershipMessage(null, MessageType.CheckAlive, null));
                        tryConnectionOutput.flush();

                        tryConnectionInput.close();
                        tryConnectionOutput.close();
                        tryConnection.close();

                        logger.warning("Alive via TCP: " + member);
                    } catch (Exception e) {
                        logger.warning("Process failure detected detected: " + member);
                        // Disseminate message first in case of false positive
                        disseminateMessage(new MembershipMessage(selfEntry, MembershipMessage.MessageType.Crash, member));

                        nodeLeave(member);
                    }
                } else {
                    logger.info("ACK received from " + member);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
