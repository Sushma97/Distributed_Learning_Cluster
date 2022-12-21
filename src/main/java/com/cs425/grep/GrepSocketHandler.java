package com.cs425.grep;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

/**
 * API to handle sending Grep requests and responses over sockets in parallel
 */
public class GrepSocketHandler {
    /**
     * Sends a grep request and receives the response.
     * Note, the function attempts to connect the socket
     *
     * @param host The socket of the server to connect to
     * @return the grep response from the server
     * @throws ClassNotFoundException
     */
    public static void grepRequest(String host, int port, GrepRequest request, CountDownLatch latch) {
        // Create a client thread for each server connection
        ClientThread client = new ClientThread(host, port, request, latch);
        // Start thread
        client.start();
    }

    /**
     * Server responds to the grep query request from client
     *
     * @param server Server socket
     * @throws IOException
     */
    public static void respondToGrepRequest(ServerSocket server) throws IOException {
        // Open resources
        Socket client = server.accept();
        //For each client connection, server starts a child thread to process the request independent of any incoming requests
        new ServerThread(client).start();
    }


}
