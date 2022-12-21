package com.cs425.grep;

import org.unix4j.Unix4j;
import org.unix4j.unix.grep.GrepOptionSet_Fcilnvx;
import org.unix4j.unix.grep.GrepOptionSets;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * Wrapper class for the Grep request object sent from client to the server. Once received by the server, performs the grep query locally
 */
public class GrepRequest implements Serializable {
    private String filename;
    private String grepPattern;
    public List<String> optionList;
    public static final long serialVersionUID = -5399605122490343339L;

    // No setters, since we don't ever want to modify a grep request in-flight.
    // We just use the constructor to set values
    public GrepRequest(String grepPattern, String filename, List<String> optionList) {
        this.grepPattern = grepPattern;
        this.filename = filename;
        this.optionList = optionList;
    }

    public String getFilename() {
        return filename;
    }

    public String getGrepPattern() {
        return grepPattern;
    }

    /**
     * Resolve the grep options provided in the CLI
     *
     * @param options list of grep options used by user
     * @return
     */
    private GrepOptionSet_Fcilnvx resolveGrepOptions(List<String> options) {
        if (options == null) {
            return null;
        }
        GrepOptionSets grepOptions = new GrepOptionSets();
        GrepOptionSet_Fcilnvx grepEnumOptions = null;
        for (String option : options) {
            if (option.equals("c")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.c;
                } else {
                    grepEnumOptions = grepEnumOptions.c;
                }
            }
            if (option.equals("F")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.F;
                } else {
                    grepEnumOptions = grepEnumOptions.F;
                }

            }
            if (option.equals("i")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.i;
                } else {
                    grepEnumOptions = grepEnumOptions.i;
                }
            }
            if (option.equals("v")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.v;
                } else {
                    grepEnumOptions = grepEnumOptions.v;
                }
            }
            if (option.equals("n")) {

                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.n;
                } else {
                    grepEnumOptions = grepEnumOptions.n;
                }

            }
            if (option.equals("l")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.l;
                } else {
                    grepEnumOptions = grepEnumOptions.l;
                }

            }
            if (option.equals("x")) {
                if (grepEnumOptions == null) {
                    grepEnumOptions = grepOptions.x;
                } else {
                    grepEnumOptions = grepEnumOptions.x;
                }

            }

        }
        return grepEnumOptions;
    }

    // Runs the grep query on server
    public GrepResponse runGrep() {
        // Fault tolerance when file not found
        // (This is treated as a RuntimeException by Unix4j.grep)
        try {
            File file = new File(filename);
            GrepOptionSet_Fcilnvx options = resolveGrepOptions(optionList);
            List<String> lines;
            if (options == null) {
                lines = Unix4j.grep(grepPattern, file).toStringList();
            } else {
                lines = Unix4j.grep(options, grepPattern, file).toStringList();
            }
            // Wrap the result of Unix4j grep in GrepResponse object
            return new GrepResponse(lines, filename);
        } catch (Exception exception) {
            return new GrepResponse(filename);
        }
    }

    @Override
    public String toString() {
        return "GrepRequest{" +
                "filename='" + filename + '\'' +
                ", grepPattern='" + grepPattern + '\'' +
                '}';

    }

    public boolean equals(GrepRequest other) {
        if (other == null) {
            return false;
        }

        if ((this.filename == null && other.filename != null) || (this.filename != null && !this.filename.equals(other.filename))) {
            return false;
        }

        if ((this.grepPattern == null && other.grepPattern != null) || (this.grepPattern != null && !this.grepPattern.equals(other.grepPattern))) {
            return false;
        }

        if ((this.optionList == null && other.optionList != null) || (this.optionList != null && !this.optionList.equals(other.optionList))) {
            return false;
        }

        return true;
    }
}


