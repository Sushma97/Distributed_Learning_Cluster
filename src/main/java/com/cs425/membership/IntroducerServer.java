package com.cs425.membership;

public class IntroducerServer {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Not enough arguments. Usage: ./introducer port");
        }

        new Introducer(Integer.parseInt(args[0])).start();
    }
}
