package com.cs425.membership;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.Properties;

/**
 * Starting point of the program
 * Deprecated, see IntroducerServer and MemberServer
 */
@Deprecated
public class BaseServer {
    public static Member member;
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Not enough arguments");
        }

        // Starting introducer only needs port number
        if (args.length == 1) {
            new Introducer(Integer.parseInt(args[0])).start();
        }

        // Starting member needs port number and introducer details
        else {
            FileInputStream config = new FileInputStream("introducer.properties");
            Properties properties = new Properties();
            properties.load(config);

            int port = Integer.parseInt(args[0]);
            String introducerHost = args[1];
            int introducerPort = Integer.parseInt(args[2]);

            member = new Member(InetAddress.getLocalHost().getHostName(), port, introducerHost, introducerPort);
            member.start();
        }
    }
}
