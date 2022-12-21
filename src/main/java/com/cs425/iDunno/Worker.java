package com.cs425.iDunno;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cs425.Messages.IDunnoMessage;
import com.cs425.Messages.IDunnoMessage.Destination;
import com.cs425.Messages.IDunnoMessage.MessageType;
import com.cs425.fileSystem.FileServer;
import com.cs425.membership.MembershipList.MemberList;

public class Worker {
    // To get coordinator and self
    private volatile MemberList memberList;
    private FileServer fileServer;

    Thread queryPollThread;
    private AtomicBoolean end;
    private AtomicBoolean started;

    public Worker(MemberList memberList, FileServer fileServer) {
        this.memberList = memberList;
        this.end = new AtomicBoolean();
        this.started = new AtomicBoolean();
        this.fileServer = fileServer;
    }

    // Handle messages to IDunno worker
    // Note: Messages are handled in their own thread, see Member.TCPListener()
    // Opening/closing resources is also handled there
    public void processIDunnoMessage(IDunnoMessage message, Socket client, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            switch (message.getMessageType()) {
                case TrainJobs:
                    if(loadModels()) {
                        outputStream.writeObject(new IDunnoMessage(MessageType.Ok, memberList.getSelfEntry(), Destination.Return));
                    } else {
                        outputStream.writeObject(new IDunnoMessage(MessageType.Fail, memberList.getSelfEntry(), Destination.Return));
                    }
                    break;

                case StartJob:
                    if (!started.get()) {
                        start();
                    }
                    break;

                default:
                    assert false : "Unexpected message type received in IDunnoCoordinator";
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean loadModels() throws IOException {
        return HttpRequest.sendTrainingRequest();
    }

    public void start() {
        started.set(true);

        queryPollThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Worker.this.queryPollProtocol();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        queryPollThread.setDaemon(true);
        queryPollThread.start();
    }

    private void getFiles(List<String> filePaths) throws InterruptedException {
        for(String filepath: filePaths) {
            Boolean completed = false;
            while(!completed) {
                completed = fileServer.processFileCommands(constants.BASE_PATH + filepath, filepath, "get", null);
//                System.out.println("Failed to fetch a file, retrying..");
                Thread.sleep(1000);
            }
        }
    }
    private void queryPollProtocol() throws InterruptedException {
        Query query = pollQuery();

        // While there are queries to process
        while (query != null && !end.get()) {
            // Log query start time
            query.logStartTime();

            //Fetch file to local from sdfs
            getFiles(query.getQueryFiles());

            // Handle query (results placed into query by HttpRequest)
            HttpRequest.sendQueryRequest(query);

            // Log query finish time
            query.logCompletionTime();

            // Send results back to coordinator
            sendCompletedQuery(query);

            // Poll for another query
            query = pollQuery();
        }

        started.set(false);
        end.set(false);
    }

    // Ask coordinator for new query
    private Query pollQuery() {
        try {
            IDunnoMessage response = IDunnoMessage.sendMessageWithResponse(
                new IDunnoMessage(
                    MessageType.PollQuery,
                    memberList.getSelfEntry(),
                    Destination.Coordinator
                ),
                memberList.getCoordinator()
            );
            return response.getQuery();
        } catch (Exception e) {
            System.out.println("Connection with coordinator failed. Trying again..."); 
            try {
                // Wait for system to converge
                Thread.sleep(5000 + (long) (Math.random() * 1000));
            } catch (Exception f) {}

            // Try again
            return pollQuery();
        }
    }

    // Send results back to coordinator
    private void sendCompletedQuery(Query query) {
        try {
            IDunnoMessage.sendMessageWithResponse(
                new IDunnoMessage(
                    MessageType.CompletedQuery,
                    memberList.getSelfEntry(),
                    Destination.Coordinator,
                    query
                ),
                memberList.getCoordinator()
            );
        } catch (Exception e) {
            System.out.println("Connection with coordinator failed. Trying again..."); 
            try {
                // Wait for system to converge
                Thread.sleep(5000 + (long) (Math.random() * 1000));
            } catch (Exception f) {}

            // Try again
            sendCompletedQuery(query);
        }
    }
}
