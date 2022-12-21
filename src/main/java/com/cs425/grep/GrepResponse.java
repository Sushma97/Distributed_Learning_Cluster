package com.cs425.grep;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class for grep response object sent from server to client.
 */
public class GrepResponse implements Serializable {
    // Matching lines from grep query
    public List<String> lines;
    private String filename;
    // Variable to find out if machine is online
    private boolean initialized;
    private boolean fileExists = true;

    public static final long serialVersionUID = -539960512249034449L;

    public GrepResponse() {
        this.initialized = false;
    }

    public GrepResponse(List<String> lines, String filename) {
        this.lines = lines;
        this.filename = filename;
        this.initialized = true;
    }

    // Constructor for when File doesn't exist
    public GrepResponse(String filename) {
        this.filename = filename;
        this.initialized = true;
        this.fileExists = false;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean fileExists() {
        return fileExists;
    }

    public void print() {
        if (!fileExists) {
            System.out.println("File not found: " + filename);
            return;
        }
        String[] filePath = filename.split("/");
        System.out.println("Grep results for " + filePath[filePath.length - 1] + ":");
        for(String line : lines) {
            System.out.println(line);
        }
    }

    public boolean equals(GrepResponse other) {
        if (other == null) {
            return false;
        }

        if ((this.filename == null && other.filename != null) || (this.filename != null && !this.filename.equals(other.filename))) {
            return false;
        }

        if ((this.lines == null && other.lines != null) || (this.lines != null && !this.lines.equals(other.lines))) {
            return false;
        }

        if (this.initialized == other.initialized && this.fileExists == other.fileExists) {
            return true;
        } else {
            return false;
        }
    }
}
