package com.cs425.grep;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Server handles each client request in a thread.
 * Responds to the grep request, runs the query and returns the results to client
 */
public class ServerThread extends Thread {

    private Socket client;

    public ServerThread(Socket client) {
        this.client = client;
    }

    public void run() {

        try {
            // Open resources
            ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());

            // Receive request
            GrepRequest grepRequest = receiveGrepRequest(inputStream);

            // Run request and send results
            System.out.println("Received request " + grepRequest);
            sendGrepResponse(grepRequest.runGrep(), outputStream);

            // Close resources
            inputStream.close();
            outputStream.close();
            client.close();
        } catch (IOException | ClassNotFoundException e) {
            // Handle any errors
            System.out.println("Error in ServerThread for client " + client.getInetAddress());
            e.printStackTrace();
        }
    }

    private static GrepRequest receiveGrepRequest(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        return (GrepRequest) inputStream.readObject();
    }

    private static void sendGrepResponse(GrepResponse response, ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(response);
        outputStream.flush();
    }
}
