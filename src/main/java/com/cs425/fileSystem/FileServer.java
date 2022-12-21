package com.cs425.fileSystem;

import java.io.*;
import java.net.Socket;
import java.util.*;

import org.apache.commons.io.FileUtils;

import com.cs425.Messages.FileMessage;
import com.cs425.Messages.FileMessage.Destination;
import com.cs425.Messages.FileMessage.MessageType;
import com.cs425.membership.MembershipList.MemberList;
import com.cs425.membership.MembershipList.MemberListEntry;
import com.cs425.iDunno.constants;

import java.util.Map.Entry;
import java.util.logging.Logger;

public class FileServer {
    public static Logger logger = Logger.getLogger("File Server");

    Random rand = new Random();
    private  String baseDirectory;
    public Set<String> files;

    // Shared from Member class
    // In order to maintain thread safety, modifiers must not be called within this class
    private volatile MemberList memberList;

    public FileServer(String host, int port, MemberList memberList) {
        // this.host = host;

        this.memberList = memberList;
        this.baseDirectory = System.getProperty("user.home")+"/Desktop/SDFS/" + host + "/" + Integer.toString(port) + "/";

        files = Collections.synchronizedSet(new HashSet<>());

        try {
            //everytime system crashes, file directory should be clean
            if (new File(baseDirectory).exists()) {
                FileUtils.cleanDirectory(new File(baseDirectory));
                FileUtils.cleanDirectory(new File(constants.BASE_PATH));
                FileUtils.cleanDirectory(new File(constants.RESULT_BASE_PATH));
            } else {
                FileUtils.forceMkdir(new File(baseDirectory));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Handle requests to file server
    // Note: Messages are handled in their own thread, see Member.TCPListener()
    // Opening/closing resources is also handled there
    public void processFileMessage(FileMessage inputMessage, Socket client, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        assert inputMessage.getDestination() == Destination.FileServer : "Recieved message not meant for this handler";
        try {
            switch (inputMessage.getMessageType()) {
                case Get:
                    if (files.contains(inputMessage.getFile())) {
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Ok, Destination.Client));
                        outputStream.flush();
                        handleGet(inputMessage.getFile(), client);
                    }
                    else {
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Fail, Destination.Client));
                        outputStream.flush();
                    }
                    break;
                case Put:
                    if (files.contains(inputMessage.getFile())) {
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Fail, Destination.Client));
                        outputStream.flush();
                    }
                    else {
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Ok, Destination.Client));
                        outputStream.flush();
                        handlePut(client,inputMessage.getFile());
                    }
                    break;
                case Replicate:
                    // Tell coordinator if node already has file
                    if (files.contains(inputMessage.getFile())) {
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.FileExists, Destination.Client));
                        outputStream.flush();
                        return;
                    }

                    boolean completed = false;

                    List<MemberListEntry> servers = inputMessage.getNodes();
                    Collections.shuffle(servers);   // Shuffle list to reduce load on single nodes

                    for(MemberListEntry node: servers) {
                        // Store with same name as received since this is an SDFS replica
                        completed = receiveFile(node, baseDirectory+inputMessage.getFile(), inputMessage.getFile());
                        if(completed){
                            files.add(inputMessage.getFile());
                            break;
                        }
                    }
                    if (!completed){
                        // Tell Coordinator operation failed
                        System.out.println("Could not complete get operation");
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Fail, Destination.Client));
                        outputStream.flush();
                    } else {
                        // Tell coordinator operation succeeded
                        outputStream.writeObject(new FileMessage(FileMessage.MessageType.Ok, Destination.Client));
                        outputStream.flush();
                    }
                    break;
                case Delete:
                    File dir = new File(baseDirectory);
                    File[] toBeDeleted = dir.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return (name.startsWith(inputMessage.getFile()));
                        }
                    });
                        for(File f : toBeDeleted)
                        {
                            f.delete();
                        }
                    files.remove(inputMessage.getFile());
                    break;
                case CoordinatorStoreRequest:
                    outputStream.writeObject(new FileMessage(FileMessage.MessageType.Ok, files, Destination.Client));
                    outputStream.flush();
                    break;
                default:
                    assert false: "Unexpected message type";
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Respond to Get request by sending file to client
    private void handleGet(String file, Socket client) {
        try{
            byte [] buffer=new byte[1024];
            Integer i;
            DataOutputStream fileOut = new DataOutputStream(client.getOutputStream());
            FileInputStream input = new FileInputStream((baseDirectory+file));
            while((i=input.read(buffer)) != -1) {
                fileOut.write(buffer,0,i);
            }
            fileOut.flush();
            fileOut.close();
            input.close();
        } catch (IOException e) {
            logger.warning("Interrupted while sending file for GET request");
        }
    }

    // Respond to Put request by storing file on local node
    private void handlePut(Socket otherFileServer, String file){
        try{
            byte [] buffer=new byte[1024];
            Integer i;
            FileOutputStream output = new FileOutputStream(baseDirectory+file);
            DataInputStream fileIn = new DataInputStream(otherFileServer.getInputStream());
            while((i=fileIn.read(buffer))>0) {
                output.write(buffer,0,i);
            }
            output.flush();
            output.close();
            fileIn.close();
            files.add(file);

        }
        catch (IOException e) {
            logger.warning("Interrupted while receiving file for PUT request");
        }
    }
    
    // Handle command line arguments from main thread
    public Boolean processFileCommands(String localFileName, String sdfsFileName, String command, Integer versions) {
        switch (command) {
            case "store":
                if (files.size() == 0){
                    System.out.println("No files stored in the current VM:");
                }
                else {
                    System.out.println("The files stored in current VM:");

                    for (String file : files) {
                        System.out.println(file.replace('#', '/'));
                    }
                }
                break;
                
            case "put":
                File file = new File(localFileName);
                if (!file.exists()) {
                    System.out.print("Put operation failed: local file not found.");
                    return false;
                }

                FileMessage putMsg = new FileMessage(FileMessage.MessageType.Put, sdfsFileName, Destination.Coordinator);
                FileMessage inputPutMessage = sendToCoordinator(putMsg);
                // Variable scoping
                {
                    int version = inputPutMessage.getVersion();
                    String putFileName = Coordinator.constructFileWithDelimiter(sdfsFileName, Integer.toString(version));

                    boolean completed = false;
                    List<MemberListEntry> servers = inputPutMessage.getNodes();
                    List<MemberListEntry> successes = new ArrayList<>();

                    for(MemberListEntry node: servers){
                        if (sendFile(node, localFileName, putFileName)) {
                            completed = true;
                            successes.add(node);
                        }
                    }

                    // Put operation must succeed for at least 1 node
                    if (!completed) {
                        sendToCoordinator(new FileMessage(FileMessage.MessageType.PutFail, sdfsFileName, successes, version, Destination.Coordinator));
                        System.out.println("Could not complete put operation");
                        return false;
                    }
                    else {
                        sendToCoordinator(new FileMessage(FileMessage.MessageType.PutOk, sdfsFileName, successes, version, Destination.Coordinator));
                        return true;
//                        System.out.println("File inserted");
                    }
                }
//                break;

            case "get":
                FileMessage getMsg = new FileMessage(FileMessage.MessageType.Get, sdfsFileName, Destination.Coordinator);
                FileMessage inputGetMessage = sendToCoordinator(getMsg);
                if(inputGetMessage.getMessageType() == FileMessage.MessageType.Fail){
                    System.out.println("GET operation failed since file does not exist in SDFS");
                    return false;
                }
                else{
                    boolean completed = false;

                    List<MemberListEntry> servers = inputGetMessage.getNodes();
                    Collections.shuffle(servers);   // Shuffle list to reduce load on single nodes

                    for(MemberListEntry node: servers){
                        completed = receiveFile(node, localFileName, inputGetMessage.getFile());
                        if(completed){
                            break;
                        }
                    }
                    if (!completed){
                        System.out.println("Could not complete get operation");
                        return false;
                    }
                    else {
                        return true;
//                        System.out.println("Fetched the file successfully");
                    }
                }
//                break;

            case "delete":
                FileMessage delMsg = new FileMessage(FileMessage.MessageType.Delete, sdfsFileName, Destination.Coordinator);
                FileMessage inputDelMessage = sendToCoordinator(delMsg);
                if(inputDelMessage.getMessageType() == FileMessage.MessageType.Fail){
                    System.out.println("Delete operation failed since file does not exist in SDFS");
                }
                else {
                    System.out.println("File Deleted");
                }
                break;

            case "ls":
                FileMessage listMsg = new FileMessage(FileMessage.MessageType.GetVersions, sdfsFileName, Destination.Coordinator);
                FileMessage inputListMessage = sendToCoordinator(listMsg);
                if(inputListMessage.getMessageType() == FileMessage.MessageType.Fail){
                    System.out.println("List operation failed since file does not exist in SDFS");
                }
                else{
                    TreeMap<Integer, Set<MemberListEntry>> servers = inputListMessage.getVersionData();
                    System.out.println();
                    for (Iterator<Entry<Integer, Set<MemberListEntry>>> iterator = servers.entrySet().iterator(); iterator.hasNext();) {
                        Entry<Integer, Set<MemberListEntry>> stack_entry = iterator.next();
                        System.out.println("The file version: " + stack_entry.getKey() + " is stored at following VM:");
                        for (Iterator<MemberListEntry> it = stack_entry.getValue().iterator(); it.hasNext();){
                            System.out.println(it.next().toString());
                        }
                        System.out.println();
                    }
                }
                break;

            case "get-versions":
                FileMessage getVMsg = new FileMessage(FileMessage.MessageType.GetVersions, sdfsFileName, Destination.Coordinator);
                FileMessage inputGetVMsg = sendToCoordinator(getVMsg);
                if(inputGetVMsg.getMessageType() == FileMessage.MessageType.Fail){
                    System.out.println("Operation failed: file does not exist in SDFS");
                }
                else {
                    TreeMap<Integer, Set<MemberListEntry>> versionStores = inputGetVMsg.getVersionData();
                    int i = 0;

                    for (Entry<Integer, Set<MemberListEntry>> pair: versionStores.descendingMap().entrySet()) {
                        // Skip if this is a pending version
                        int version = pair.getKey();
                        Set<MemberListEntry> servers = pair.getValue();
                        if (servers.isEmpty()) {
                            continue;
                        }

                        // Only get 5 most recent versions
                        if (i >= 5) {
                            break;
                        }
                        i++;

                        String sdfsFileNameWithVersion = Coordinator.constructFileWithDelimiter(sdfsFileName, Integer.toString(version));
                        String localFileNameWithVersion = Coordinator.constructFileWithDelimiterAndFullPath(localFileName, Integer.toString(version));

                        for(MemberListEntry node: servers){
                            if (receiveFile(node, localFileNameWithVersion, sdfsFileNameWithVersion)) {
                                System.out.println("Version " + version + " retrieved successfully");
                                break;
                            } else {
                                System.out.println("Failed to retrieve version " + version + " from " + node);
                            }
                        }
                    }
                }
                break;

            default:
                break;
        }
        return true;
    }

    // Send message to coordinator and return response
    private FileMessage sendToCoordinator(Serializable message) {
        // Get coordinator
        MemberListEntry coordinator = memberList.getCoordinator();

        try {
            // Open resources
            Socket coordinatorSocket = new Socket(coordinator.getHostname(), coordinator.getPort());
            ObjectInputStream input = new ObjectInputStream(coordinatorSocket.getInputStream());
            ObjectOutputStream output = new ObjectOutputStream(coordinatorSocket.getOutputStream());

            // Write message
            output.writeObject(message);
            output.flush();

            // Receive response
            FileMessage coordinatorResponse = (FileMessage) input.readObject();

            // close resources
            output.close();
            input.close();
            coordinatorSocket.close();

            // Return response
            return coordinatorResponse;

        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Connection with coordinator failed. Trying again..."); 
            try {
                if (!memberList.getBackupPromoted()) {
                    // Wait for backup coordinator to take over
                    memberList.wait();
                } else {
                    // Wait for system to converge
                    Thread.sleep(5000 + (long) (Math.random() * 1000));
                }
            } catch (Exception f) {}

            // Try again
            return sendToCoordinator(message);
        }
    }

    // Recieve file response from a file server for Get request
    private boolean receiveFile(MemberListEntry entry, String localFileName, String sdfsFileName){
        try {
            // Open resources
            if (!memberList.containsEntry(entry)){
                return false;
            }
            Socket otherFileServer = new Socket(entry.getHostname(), entry.getPort());
            ObjectOutputStream output = new ObjectOutputStream(otherFileServer.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(otherFileServer.getInputStream());
            FileMessage outputMsg = new FileMessage(FileMessage.MessageType.Get, sdfsFileName, Destination.FileServer);
            // Send Get request to file server containing file
            output.writeObject(outputMsg);
            output.flush();

            // Receive response
            try {
                FileMessage response = (FileMessage) input.readObject();
                if (response.getMessageType() != MessageType.Ok) {
                    System.out.println("Get request failed: node does not contain file");
                    return false;
                }
            } catch (Exception e) {
                logger.warning("Connection interrupted while receiving file from " + entry.getHostname());
                return false;
            }

            // Recieve file
            DataInputStream fileInput = new DataInputStream(otherFileServer.getInputStream());
            byte[] buffer = new byte[1024];
            FileOutputStream fileOutput = new FileOutputStream(localFileName);
            Integer i;
            while((i=fileInput.read(buffer)) > 0) {
                fileOutput.write(buffer,0,i);

            }
            fileOutput.flush();

            // Close resources
            fileOutput.close();
            fileInput.close();
            output.close();
            input.close();
            otherFileServer.close();
        }
        catch (IOException e) {
            logger.warning("Connection interrupted while receiving file from " + entry.getHostname());
            return false;
        }
        return true;
    }

    // Send file to FileServer to store for Put request
    private boolean sendFile(MemberListEntry entry, String localFileName, String sdfsFileName){
        try {
            // Open resources
            Socket memberServer = new Socket(entry.getHostname(), entry.getPort());
            ObjectOutputStream output = new ObjectOutputStream(memberServer.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(memberServer.getInputStream());

            // Send Put request to file server that should store file
            output.writeObject(new FileMessage(FileMessage.MessageType.Put, sdfsFileName, Destination.FileServer));
            output.flush();

            // Receive response
            try {
                FileMessage response = (FileMessage) input.readObject();
                if (response.getMessageType() != MessageType.Ok) {
                    System.out.println("Put request failed: node already contains file");
                    return false;
                }
            } catch (Exception e) {
                logger.warning("Connection interrupted while sending file to " + entry.getHostname());
                return false;
            }

            // Send file
            DataOutputStream fileOutput = new DataOutputStream(memberServer.getOutputStream());
            byte[] buffer = new byte[1024];
            FileInputStream fileInputStream = new FileInputStream(localFileName);
            Integer i;
            while((i=fileInputStream.read(buffer)) != -1) {
                fileOutput.write(buffer,0,i);
            }

            // Close resources
            fileOutput.flush();
            fileOutput.close();
            fileInputStream.close();
            output.close();
            input.close();
            memberServer.close();
        }
        catch (IOException e) {
            logger.warning("Connection interrupted while sending file to " + entry.getHostname());
            return false;
        }
        return true;
    }
}
