package com.cs425.fileSystem;

import com.cs425.Messages.FileMessage;
import com.cs425.Messages.FileMessage.Destination;
import com.cs425.Messages.FileMessage.MessageType;
import com.cs425.iDunno.IDunnoCoordinator;
import com.cs425.membership.MembershipList.MemberList;
import com.cs425.membership.MembershipList.MemberListEntry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class Coordinator extends Thread {
    public static Logger logger = Logger.getLogger("Coordinator");

    public static final int REPLICATION_COUNT = 4;
    private static final long REPLICATION_PERIOD = 10000;
    public static final char DELIMITER = '_';

    private volatile Map<String, TreeMap<Integer, Set<MemberListEntry>>> fileStorage;

    // Shared from Member class
    // In order to maintain thread safety, modifiers must not be called within this class
    private volatile MemberList memberList;

    private Thread coordinatorThread;
    private AtomicBoolean end;

    // Create coordinator instance and begin replication
    public Coordinator(MemberList memberList, AtomicBoolean end) {
        this.fileStorage = Collections.synchronizedMap(new HashMap<>());
        this.memberList = memberList;
        this.end = end;

        // Get all files if elected coordinator
        requestAllFiles();
        initialize();
    }

    // Create coordinator instance with existing file store and begin replication
    public Coordinator(MemberList memberList, AtomicBoolean end, IDunnoCoordinator coord) {
        this.memberList = memberList;
        this.end = end;
        this.fileStorage = Collections.synchronizedMap(coord.getFileStore());
        coord.setFileStore(this.fileStorage);

        initialize();
    }

    private void initialize() {
        // Start background replication
        coordinatorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Coordinator.this.backgroundReplication();
            }
        });
        coordinatorThread.setDaemon(true);
        coordinatorThread.start();
        logger.info("Coordinator replication checker started");
    }

    private void requestAllFiles() {
        // Get existing data from nodes if this is an elected coordinator
        FileMessage coordinatorDataStoreRequest = new FileMessage(MessageType.CoordinatorStoreRequest, Destination.FileServer);
        for (MemberListEntry node: memberList.getMemberList()) {
            FileMessage response = sendToFileServerWithResponse(node, coordinatorDataStoreRequest);
            if (response != null) {
                addFilesToDataStore(response.getFilesOnNode(), node);
            }
        }
    }

    public Map<String, TreeMap<Integer, Set<MemberListEntry>>> getFileStore() {
        return fileStorage;
    }

    // Add file meta data to coordinator data store from file names at given node delimited with version number
    private void addFilesToDataStore(Set<String> files, MemberListEntry node) {
        for (String file: files) {
            MutablePair<String, Integer> pair = deconstructFileWithDelimiter(file);
            addFileToDataStore(pair.getLeft(), pair.getRight(), node);
        }
    }

    // Add file metadata to coordinator data store for a given node
    private void addFileToDataStore(String filename, int version, MemberListEntry node) {
        synchronized (fileStorage) {
            TreeMap<Integer, Set<MemberListEntry>> fileMap = fileStorage.getOrDefault(filename, new TreeMap<>());
            Set<MemberListEntry> versionSet = fileMap.getOrDefault(version, new HashSet<>());
            versionSet.add(node);
            fileMap.put(version, versionSet);
            fileStorage.put(filename, fileMap);
        }
    }

    // Given a file name and a version, constructs a new filename using the given version, ignoring path
    public static String constructFileWithDelimiter(String fileName, String version){
        String baseName = FilenameUtils.getBaseName(fileName);
        String extension = FilenameUtils.getExtension(fileName);
        return  baseName + DELIMITER + version + (extension.length() > 0 ? '.' + extension : "");
    }

    // Given a file name and a version, constructs a new filename using the given version, including the full path
    public static String constructFileWithDelimiterAndFullPath(String fileName, String version){
        String fullPath = FilenameUtils.getFullPath(fileName);
        String baseName = FilenameUtils.getBaseName(fileName);
        String extension = FilenameUtils.getExtension(fileName);
        return  fullPath + baseName + DELIMITER + version + (extension.length() > 0 ? '.' + extension : "");
    }

    // given the SDFS file name containing version, deconstructs to get original SDFS file name and version
    public static MutablePair<String, Integer> deconstructFileWithDelimiter(String fileName){
        String baseName = FilenameUtils.getBaseName(fileName);
        String extension = FilenameUtils.getExtension(fileName);

        int delimiterIndex = baseName.lastIndexOf(DELIMITER);

        String originalName = baseName.substring(0, delimiterIndex) + (extension.length() > 0 ? '.' + extension : "");
        Integer version = Integer.parseInt(baseName.substring(delimiterIndex + 1));

        return  MutablePair.of(originalName, version);
    }

    // Retrieve the nodes at which the latest version of a file is stored
    public MutablePair<String, List<MemberListEntry>> getLatestVersion(String filename) {
        if(!fileStorage.containsKey(filename)) {
            return null;
        }

        synchronized (fileStorage) {
            TreeMap<Integer, Set<MemberListEntry>> fileVersions = fileStorage.get(filename);
            for (Entry<Integer, Set<MemberListEntry>> entry: fileVersions.descendingMap().entrySet()) {
                if (entry.getValue().size() > 0) {
                    return MutablePair.of(constructFileWithDelimiter(filename, entry.getKey().toString()), new ArrayList<>(entry.getValue()));
                }
            }
        }

        return null;
    }

    // Handle requests to file server
    // Note: Messages are handled in their own thread, see Member.TCPListener()
    // Opening/closing resources is also handled there
    public void processFileMessage(FileMessage inputMessage, Socket client, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try{
            String fileName = inputMessage.getFile();

            switch (inputMessage.getMessageType()) {
                case Ok:
                    break;

                case Get:
                    MutablePair<String, List<MemberListEntry>> latestVersion =  getLatestVersion(fileName);
                    if (latestVersion == null) {
                        FileMessage msg = new FileMessage(FileMessage.MessageType.Fail, Destination.Client);
                        sendToClient(msg, inputStream, outputStream);
                    } else {
                        FileMessage msg = new FileMessage(FileMessage.MessageType.Ok, latestVersion.getLeft(), latestVersion.getRight(), Destination.Client);
                        sendToClient(msg, inputStream, outputStream);
                    }
                    break;

                case Put:
                    List<MemberListEntry> memberListEntries = memberList.getMemberList();
                    Collections.shuffle(memberListEntries);
                    memberListEntries = new ArrayList<>(memberListEntries.subList(0, Math.min(memberListEntries.size(), REPLICATION_COUNT)));
                    
                    // If already stored, new version is latest version + 1, otherwise 1
                    int newVersion = fileStorage.containsKey(fileName) ? newVersion = fileStorage.get(fileName).lastKey() + 1 : 1;

                    // Create empty entry for new version to reserve version number
                    synchronized (fileStorage) {
                        TreeMap<Integer, Set<MemberListEntry>> dataFiles = fileStorage.getOrDefault(fileName, new TreeMap<>());
                        dataFiles.put(newVersion, new HashSet<>());
                        fileStorage.put(fileName, dataFiles);
                    }

                    // Variable scoping
                    {
                        FileMessage msg = new FileMessage(FileMessage.MessageType.Ok, fileName, memberListEntries, newVersion, Destination.Client);
                        sendToClient(msg, inputStream, outputStream);
                    }
                    break;

                case PutOk:
                    // Update metadata on put success
                    List<MemberListEntry> putSuccessNodes = inputMessage.getNodes();
                    int version = inputMessage.getVersion();
                    synchronized (fileStorage) {
                        fileStorage.get(fileName).put(version, new HashSet<>(putSuccessNodes));
                    }
                    sendToClient(new FileMessage(MessageType.Ok, Destination.Client), inputStream, outputStream);
                    break;

                case PutFail:
                    // Delete reserved entry from metadata on put failure
                    version = inputMessage.getVersion();
                    synchronized (fileStorage) {
                        fileStorage.get(fileName).remove(version);
                    }
                    sendToClient(new FileMessage(MessageType.Ok, Destination.Client), inputStream, outputStream);
                    break;

                case Delete:
                    if(!fileStorage.containsKey(fileName)){
                        FileMessage msg = new FileMessage(FileMessage.MessageType.Fail, Destination.Client);
                        sendToClient(msg, inputStream, outputStream);
                    }
                    else {
                        TreeMap<Integer, Set<MemberListEntry>> fileValues = fileStorage.remove(fileName);

                        for(Entry<Integer, Set<MemberListEntry>> location: fileValues.entrySet()) {
                            String deleteFileName = constructFileWithDelimiter(fileName, location.getKey().toString());
                            FileMessage deleteMsg = new FileMessage(FileMessage.MessageType.Delete, deleteFileName, Destination.FileServer);
                            for (MemberListEntry memberListEntry : location.getValue()) {
                                sendToFileServer(memberListEntry, deleteMsg);
                            }
                        }

                        FileMessage newMsg = new FileMessage(FileMessage.MessageType.Ok, fileName, Destination.Client);
                        sendToClient(newMsg, inputStream, outputStream);
                    }
                    break;

                case GetVersions:
                    if(!fileStorage.containsKey(fileName)) {
                        FileMessage versionMsg = new FileMessage(FileMessage.MessageType.Fail, Destination.Client);
                        sendToClient(versionMsg, inputStream, outputStream);
                    } else {
                        TreeMap<Integer, Set<MemberListEntry>> fileValues = fileStorage.get(fileName);
                        FileMessage  versionMsg = new FileMessage(FileMessage.MessageType.Ok, fileName, fileValues, Destination.Client);
                        sendToClient(versionMsg, inputStream, outputStream);
                    }
                    break;

                default:
                    break;
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
    }

    // Send message back to client
    private static void sendToClient(Object message, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            // Write message
            outputStream.writeObject(message);
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Forward instruction message to file server
    // Fire and forget
    private static void sendToFileServer(MemberListEntry fileServer, Serializable message) {
        try {
            // Open resources
            Socket fileServerSocker = new Socket(fileServer.getHostname(), fileServer.getPort());
            ObjectOutputStream output = new ObjectOutputStream(fileServerSocker.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(fileServerSocker.getInputStream());

            // Write message
            output.writeObject(message);
            output.flush();

            // Close resources
            inputStream.close();
            output.close();
            fileServerSocker.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Forward instruction message to file server
    // Receives response from file server
    private static FileMessage sendToFileServerWithResponse(MemberListEntry fileServer, Serializable message) {
        FileMessage response = null;
        try {
            // Open resources
            Socket fileServerSocket = new Socket(fileServer.getHostname(), fileServer.getPort());
            ObjectOutputStream output = new ObjectOutputStream(fileServerSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(fileServerSocket.getInputStream());

            // Write message
            output.writeObject(message);
            output.flush();

            // Recieve response
            response = (FileMessage) inputStream.readObject();

            // Close resources
            inputStream.close();
            output.close();
            fileServerSocket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return response;
    }

    public static Map<String, TreeMap<Integer, Set<MemberListEntry>>> synchronizedDeepFileStorageCopy(Map<String, TreeMap<Integer, Set<MemberListEntry>>> filestore) {
        synchronized (filestore) {
            Map<String, TreeMap<Integer, Set<MemberListEntry>>> newFilestore = new HashMap<>();

            for (Entry<String, TreeMap<Integer, Set<MemberListEntry>>> entry : filestore.entrySet()) {
                TreeMap<Integer, Set<MemberListEntry>> intermediate = new TreeMap<>();

                for (Entry<Integer, Set<MemberListEntry>> entry2: entry.getValue().entrySet()) {
                    intermediate.put(entry2.getKey(), new TreeSet<>(entry2.getValue()));
                }

                newFilestore.put(new String(entry.getKey()), intermediate);
            }

            return newFilestore;
        }
    }

    // Replicate files as needed. Run as a separate thread
    private void backgroundReplication() {
        while (!end.get()) {
            // Acquire copy of fileStorage for use (prevents concurrent modification)
            Set<Entry<String, TreeMap<Integer, Set<MemberListEntry>>>> files = synchronizedDeepFileStorageCopy(fileStorage).entrySet();

            for (Entry<String, TreeMap<Integer, Set<MemberListEntry>>> entry : files) {
                String fileName = entry.getKey();

                // In case of removal before this iteration reached
                if (!fileStorage.containsKey(fileName)) {
                    continue;
                }

                // For each version of this file: Version -> <Machine Locations>
                for (Entry<Integer, Set<MemberListEntry>> fileVersionState : entry.getValue().entrySet()) {
                    Set<MemberListEntry> members = fileVersionState.getValue();

                    // Skip pending versions
                    if (members.isEmpty()) {
                        continue;
                    }

                    // Remove failed machines that stored this version of this file
                    for (Iterator<MemberListEntry> memberIterator = members.iterator(); memberIterator.hasNext();) {
                        // If machine not present in membership list, then this node has failed and file must be replicated
                        MemberListEntry memberListEntry = memberIterator.next();
                        if(!memberList.containsEntry(memberListEntry)) {
                            memberIterator.remove();
                        }
                    }

                    if (REPLICATION_COUNT - members.size() > 0) {
                        // Get random permutation of enough members to replicate this file
                        List<MemberListEntry> memberListEntries = memberList.getMemberList();
                        memberListEntries.removeAll(members);   // Remove members this file is already stored at from potential replicas
                        Collections.shuffle(memberListEntries);
                        memberListEntries = new ArrayList<>(memberListEntries.subList(0, Math.min(REPLICATION_COUNT - members.size(), memberListEntries.size())));

                        // Replicate file at each chosen machine
                        String newFileName = constructFileWithDelimiter(fileName, fileVersionState.getKey().toString());
                        for(MemberListEntry putFileEntry: memberListEntries) {
                            FileMessage msg = new FileMessage(FileMessage.MessageType.Replicate, newFileName, new ArrayList<MemberListEntry>(members), Destination.FileServer);

                            FileMessage response = sendToFileServerWithResponse(putFileEntry, msg);
                            if (response == null) {
                                System.out.println("Failed replication of " + newFileName + " at " + putFileEntry + " failed:");
                                System.out.println("Node not active");
                            } else if (response.getMessageType() == MessageType.Ok) {
                                members.add(putFileEntry);
                            } else {
                                switch (response.getMessageType()) {
                                    case Fail:
                                        System.out.println("Failed replication of " + newFileName + " at " + putFileEntry + " failed:");
                                        System.out.println("File not received");
                                        break;
                                    case FileExists:
                                        members.add(putFileEntry);
                                        break;
                                    default:
                                        assert false: "Unexpected message type received";
                                        break;
                                }
                            }
                        }
                    }

                    // Update state to reflect new replicas, if key still exists
                    if (fileStorage.containsKey(fileName)) {
                        fileStorage.get(fileName).put(fileVersionState.getKey(), members);
                    }
                }
            }
            // Sleep for replication period
            try {
                sleep(REPLICATION_PERIOD);
            } catch (InterruptedException e) {
                continue;
            }
        }
    }
}
