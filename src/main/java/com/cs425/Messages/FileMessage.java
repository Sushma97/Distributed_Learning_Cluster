package com.cs425.Messages;

import com.cs425.membership.MembershipList.MemberListEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

public class FileMessage implements Serializable {
    public enum MessageType {
        Get,
        Put,
        PutOk,
        PutFail,
        Delete,
        Store,
        GetVersions,
        Replicate,
        Ok,
        Fail,
        FileExists,
        CoordinatorStoreRequest
    }

    public enum Destination {
        Coordinator,
        FileServer,
        Client
    }

    private FileMessage.MessageType messageType;
    private Destination destination;

    private String file;
    private List<MemberListEntry> nodes;
    private int version;
    private TreeMap<Integer, Set<MemberListEntry>>  versionData;

    // For use with CoordinatorStoreRequest
    private Set<String> filesOnNode;

    public List<MemberListEntry> getNodes() {
        return nodes;
    }

    public TreeMap<Integer, Set<MemberListEntry>>  getVersionData() {
        return versionData;
    }

    public FileMessage(FileMessage.MessageType messageType, String file, Destination destination) {
        this.messageType = messageType;
        this.destination = destination;
        this.file = file;
        this.nodes = new ArrayList<>();
        this.versionData = new TreeMap<>();
    }

    public FileMessage(FileMessage.MessageType messageType, String file, TreeMap<Integer, Set<MemberListEntry>> versionData, Destination destination) {
        this.messageType = messageType;
        this.destination = destination;
        this.file = file;
        this.nodes = new ArrayList<>();
        this.versionData = versionData;
    }

    public FileMessage(FileMessage.MessageType messageType, Destination destination) {
        this.messageType = messageType;
        this.destination = destination;
        this.file = "";
        this.nodes = new ArrayList<>();
        this.versionData = new TreeMap<>();
    }

    public FileMessage(FileMessage.MessageType messageType, String file, List<MemberListEntry> nodes, Destination destination) {
        this.messageType = messageType;
        this.destination = destination;
        this.file = file;
        this.nodes = nodes;
        this.versionData = new TreeMap<>();
    }

    public FileMessage(FileMessage.MessageType messageType, Set<String> filesOnNode, Destination destination) {
        this.messageType = messageType;
        this.filesOnNode = filesOnNode;
    }

    // For Put, PutOk, and PutFail
    public FileMessage(FileMessage.MessageType messageType, String file, List<MemberListEntry> nodes, int version, Destination destination) {
        this.messageType = messageType;
        this.destination = destination;
        this.file = file;
        this.nodes = nodes;
        this.version = version;
    }

    public FileMessage.MessageType getMessageType() {
        return messageType;
    }

    public Destination getDestination() {
        return destination;
    }

    public String getFile() {
        return file;
    }

    public Set<String> getFilesOnNode() {
        return filesOnNode;
    }

    public int getVersion() {
        return version;
    }
}
