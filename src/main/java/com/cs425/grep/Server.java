package com.cs425.grep;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Class that launches server and responds to client query request parallel.
 */
public class Server {
    private static ServerSocket server;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Please input the port number server should run on");
        }
        server = new ServerSocket(Integer.parseInt(args[0]));

        // Unterminating loop
        while (true) {
            // Accept a socket connection
            System.out.println("Waiting for client grep request");
            // Handles each request in separate thread
            GrepSocketHandler.respondToGrepRequest(server);
        }
    }
}
