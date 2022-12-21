package com.cs425.membership;

import com.cs425.Messages.MembershipMessage;
import com.cs425.Messages.MembershipMessage.MessageType;
import com.cs425.membership.MembershipList.MemberListEntry;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Introducer to facilitate node/machine joins
 */
public class Introducer {

    Queue<MemberListEntry> recentJoins;
    int port;

    // Logger
    public static Logger logger = Logger.getLogger("IntroducerLogger");

    public Introducer(int port) throws SecurityException, IOException {
        this.port = port;
        recentJoins = new LinkedList<MemberListEntry>();

        Handler fh = new FileHandler("/srv/mp2_logs/introducer.log");
//        Handler fh = new FileHandler("./introducer.log");
        fh.setFormatter(new SimpleFormatter());
        logger.setUseParentHandlers(false);
        logger.addHandler(fh);
    }

    public void start() throws InterruptedException {
        logger.info("Introducer started");
        Joiner newjoin = new Joiner(this.port);
        newjoin.start();
    }

    // Each join process is executed in a thread
    private class Joiner extends Thread {
        private int port;
        private AtomicBoolean end;
        public Joiner(int port) {
            this.port = port;
            end = new AtomicBoolean(
                    false
            );
        }

        @Override
        public void run() {
            ServerSocket server;
            try {
                server = new ServerSocket(this.port);

                while (!end.get()){
                    logger.info("Waiting for request at " + server.toString());
                    // Accept join request
                    Socket request = server.accept();
                    logger.info("Connection established on port " + request.getLocalPort() + " with " + request.toString());
                    // Create resources
                    ObjectOutputStream output = new ObjectOutputStream(request.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(request.getInputStream());
                    logger.info("IO Streams created");

                    MemberListEntry newEntry;
                    try {
                        newEntry = (MemberListEntry) input.readObject();
                        logger.info("Member joining: " + newEntry.toString());
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        continue;
                    }

                    // Connect to non-faulty process
                    MemberListEntry groupMember = recentJoins.peek();
                    while (groupMember != null) {
                        try {
                            Socket tryConnection = new Socket(groupMember.getHostname(), groupMember.getPort());
                            logger.info("Found living process: " + groupMember.getHostname() + ":" + groupMember.getPort());

                            ObjectOutputStream tryConnectionOutput = new ObjectOutputStream(tryConnection.getOutputStream());
                            ObjectInputStream tryConnectionInput = new ObjectInputStream(tryConnection.getInputStream());

                            MembershipMessage checkAlive = new MembershipMessage(null, MessageType.CheckAlive, null);

                            tryConnectionOutput.writeObject(checkAlive);
                            tryConnectionOutput.flush();

                            logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(checkAlive) + " bytes over TCP");

                            tryConnectionInput.close();
                            tryConnectionOutput.close();
                            tryConnection.close();
                            break;
                        } catch (Exception e) {
                            // remove faulty/left process and choose next one
                            logger.info("Process no longer joined: " + groupMember.getHostname() + ":" + groupMember.getPort());
                            recentJoins.poll();
                            groupMember = recentJoins.peek();
                        }
                    }

                    output.writeObject(groupMember);
                    logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(groupMember) + " bytes over TCP");
                    if (groupMember != null) {
                        logger.info("Living process sent to newly joined process");
                    } else {
                        logger.info("Newly joined process is the first group member");
                    }
                    
                    output.flush();

                    input.close();
                    output.close();
                    request.close();

                    recentJoins.add(newEntry);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


}
