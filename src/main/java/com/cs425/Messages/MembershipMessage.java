package com.cs425.Messages;

import java.io.Serializable;

import com.cs425.membership.MembershipList.MemberListEntry;

/**
 * Communication message. Contains message type and the member details
 */
public class MembershipMessage implements Serializable {
    public enum MessageType {
        Join,
        Leave,
        Crash,
        Ping,
        Ack,
        MemberListRequest,
        CheckAlive,
        // ElectionId,
        // Elected
    }

    private MemberListEntry sender;
    private MessageType messageType;
    private MemberListEntry subjectEntry;

    public MembershipMessage(MemberListEntry sender, MessageType messageType, MemberListEntry subjectEntry) {
        this.sender = sender;
        this.messageType = messageType;
        this.subjectEntry = subjectEntry;
    }

    public MemberListEntry getSender() {
        return sender;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public MemberListEntry getSubjectEntry() {
        return subjectEntry;
    }
}
