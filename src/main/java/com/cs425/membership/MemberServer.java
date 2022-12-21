package com.cs425.membership;

import java.net.InetAddress;
import java.util.Properties;

public class MemberServer {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Not enough arguments. Usage: ./member port");
        }

        // Load introducer location from config file
        Properties properties = new Properties();
        properties.load(MemberServer.class.getClassLoader().getResourceAsStream("introducer.properties"));

        // Read port from argument
        int port = Integer.parseInt(args[0]);

        String introducerHost = properties.getProperty("INTRODUCER_HOST");
        int introducerPort = Integer.parseInt(properties.getProperty("INTRODUCER_PORT"));

        new Member(InetAddress.getLocalHost().getHostName(), port, introducerHost, introducerPort).start();
    }
}
