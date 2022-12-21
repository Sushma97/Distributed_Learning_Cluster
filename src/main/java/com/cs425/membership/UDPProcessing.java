package com.cs425.membership;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
    Utility class to send and recieve UDP packets
 */
public class UDPProcessing {
    public static void sendPacket(DatagramSocket socket, Serializable object, String host, int port) throws IOException {
        DatagramPacket packet = convertToPacket(object, InetAddress.getByName(host), port);
        socket.send(packet);
        Member.logger.info("Sent " + packet.getLength() + " bytes over UDP");
    }

    public static Object receivePacket(DatagramSocket socket) throws IOException, ClassNotFoundException {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);

        return convertFromPacket(packet);
    }

    private static DatagramPacket convertToPacket(Serializable object, InetAddress host, int port) {
        byte[] bytes = SerializationUtils.serialize(object);
        return new DatagramPacket(bytes, bytes.length, host, port);
    }

    private static Object convertFromPacket(DatagramPacket packet) {
        return SerializationUtils.deserialize(packet.getData());
    }

}
