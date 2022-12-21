package com.cs425.membership.MembershipList;

import java.io.Serializable;
import java.util.Date;

/**
 * Individual member details
 */
public class MemberListEntry implements Serializable, Comparable<MemberListEntry> {
    private String hostname;
    private int port;
    // Distinguishes Incarnation
    private Date timestamp;

    public MemberListEntry(String hostname, int port, Date timestamp) {
        assert(hostname != null);
        assert(timestamp != null);

        this.hostname = hostname;
        this.port = port;
        this.timestamp = timestamp;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object otherObj) {
        if (!(otherObj instanceof MemberListEntry)) {
            return false;
        }

        MemberListEntry other = (MemberListEntry) otherObj;
        return this.hostname.equals(other.hostname)
                    && this.port == other.port
                    && this.timestamp.equals(other.timestamp);
    }

    @Override
    public int hashCode() {
        return timestamp.hashCode();
    }

    @Override
    public int compareTo(MemberListEntry other) {
        if (!this.timestamp.equals(other.timestamp)) {
            return this.timestamp.compareTo(other.timestamp);
        } else if (!this.hostname.equals(other.hostname)) {
            return this.hostname.compareTo(other.hostname);
        } else {
            return this.port - other.port;
        }
    }

    @Override
    public String toString() {
        return this.hostname + "\t" + this.port + "\t" + this.timestamp.toString();
    }
}
