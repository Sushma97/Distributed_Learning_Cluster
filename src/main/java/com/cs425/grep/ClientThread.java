package com.cs425.grep;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client call to each server is in a separate thread.
 * Sends the grep request to server and prints the received response
 */
public class ClientThread extends Thread {
    private String ip;
    private Integer port;
    private GrepRequest request;
    private volatile GrepResponse response = null;
    //Variable to track the total count of matching lines.
    public static int totalCount = 0;
    //Variable to identify when all the client threads are completed
    public CountDownLatch latch;

    private static Lock printLock = new ReentrantLock();

    public ClientThread(String ip, Integer port, GrepRequest request, CountDownLatch latch) {
        this.ip = ip;
        this.port = port;
        this.request = request;
        this.latch = latch;
    }

    public void run() {
        GrepResponse response;
        try {
            // Open resources
            Socket server = new Socket(ip, port);
            ObjectOutputStream outputStream = new ObjectOutputStream(server.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(server.getInputStream());
            //Send the grep request to server
            sendGrepRequest(request, outputStream);
            //Fetch the grep response from server
            response = receiveGrepResponse(inputStream);
            synchronized (ClientThread.class) {
                //If the response was successful then find out the count of matching lines
                if (response.lines != null) {
                    //If count flag is used, add the count value in the response
                    if (request.optionList.contains("c")) {
                        totalCount += Integer.parseInt(response.lines.get(0));
                    } else {
                        totalCount += response.lines.size();
                    }
                }
            }
            // Close resources
            inputStream.close();
            outputStream.close();
            server.close();
        } catch (ConnectException exception) {
            // Error handling for fault tolerance if connection refused or unavailable
            // Return uninitialized GrepResponse, so caller knows no connection was established
            response = new GrepResponse();
        } catch (IOException | ClassNotFoundException e) {
            // Some other error occurred
            System.out.println("Error in ClientThread for server " + ip);
            e.printStackTrace();
            return;
        } finally {
            // Track the end of each thread
            if (latch != null) latch.countDown();
        }

        // Save result
        this.response = response;

        // Print results
        printLock.lock();
        if (response.isInitialized()) {
            System.out.println(ip + ":");
            response.print();
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        } else {
            System.out.println("Machine (IP: " + ip + ", Port: " + port + ") offline.");
        }
        printLock.unlock();
    }

    public GrepResponse getGrepResponse() {
        return this.response;
    }

    private static void sendGrepRequest(GrepRequest request, ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(request);
        outputStream.flush();
    }

    private static GrepResponse receiveGrepResponse(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        return (GrepResponse) inputStream.readObject();
    }
}
