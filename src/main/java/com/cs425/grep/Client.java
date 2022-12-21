package com.cs425.grep;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Class that launches client with the ability to query logs on distributed machines
 */
public class Client {

    /**
     * Fetches server details
     *
     * @param filename json file containing server details
     * @return list of Machine.
     * @throws IOException
     * @throws URISyntaxException
     */
    public static List<MachineLocation> getServerList(String filename) {
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jn = mapper.readValue(stream, JsonNode.class);
            String serverList = mapper.writeValueAsString(jn);
            List<MachineLocation> list = mapper.readValue(serverList, new TypeReference<List<MachineLocation>>() {
            });
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException{
        // Fetch server details
        List<MachineLocation> list = getServerList("server.json");
        CommandLineInput cli = new CommandLineInput(args);
        // Client creates GrepRequest based on arguments
        String grepPattern = cli.pattern;
        // Tracks the number of threads
        int latchGroupCount = list.size();
        CountDownLatch latch = new CountDownLatch(latchGroupCount);
        // Client sends out GrepRequest over sockets to each server
        for (MachineLocation machine : list) {
            // Initialize grep request
            GrepRequest request;
            if (cli.filename != null) {
                request = new GrepRequest(grepPattern, cli.filename, cli.optionList);
            } else {
                request = new GrepRequest(grepPattern, machine.getLogFile(), cli.optionList);
            }
            System.out.println("Sending the grepRequest " + request);
            // Send request and print results
            GrepSocketHandler.grepRequest(machine.getIp(), machine.getPort(), request, latch);
        }
        // Waits for all the threads to finish their process
        latch.await();
        // Print total number of matching lines
        System.out.println("Total matching lines count is: " + ClientThread.totalCount);
    }

    /**
     * Parses the command line arguments
     */
    private static class CommandLineInput {
        // Variable to hold the grep options used in CLI
        public List<String> optionList;
        // Variable to hold the pattern to search for
        public String pattern;
        public String filename;

        public CommandLineInput(String[] args) throws ParseException {
            Options options = generateOptions();
            CommandLineParser parser = new DefaultParser();
            // Parse CLI for options
            CommandLine cmd = parser.parse(options, args);
            optionList = new ArrayList<>();
            pattern = null;
            filename = null;
            // Pattern is required
            if (!cmd.hasOption("pattern")) {
                throw new ParseException("search input is a required argument");
            }

            if (cmd.hasOption("c")) {
                optionList.add("c");
            }
            if (cmd.hasOption("F")) {
                optionList.add("F");
            }
            if (cmd.hasOption("i")) {
                optionList.add("i");
            }
            if (cmd.hasOption("v")) {
                optionList.add("v");
            }
            if (cmd.hasOption("n")) {
                optionList.add("n");
            }
            if (cmd.hasOption("l")) {
                optionList.add("l");
            }
            if (cmd.hasOption("x")) {
                optionList.add("x");
            }
            if (cmd.hasOption("pattern")) {
                pattern = cmd.getOptionValue("pattern");
            }
            if (cmd.hasOption("file")) {
                filename = cmd.getOptionValue("file");
            }

        }
    }

    /**
     * Generate the options for grep
     *
     * @return Grep options
     */
    private static Options generateOptions() {
        Options options = new Options();
        options.addOption(new Option("c", "grep option to count matching lines"));
        options.addOption(new Option("F", "grep option to use fixed-strings matching instead of regular expressions"));
        options.addOption(new Option("i", "grep option to ignore case"));
        options.addOption(new Option("v", "grep option to invert match"));
        options.addOption(new Option("n", "grep option to prefix each line of output with the line number within its input file"));
        options.addOption(new Option("l", "grep option to print the name of each input file"));
        options.addOption(new Option("x", "grep option to match whole sentence only"));
        options.addOption(Option.builder().argName("pattern").hasArg().desc("*Required Option* grep string").option("pattern").required().build());
        options.addOption(Option.builder().argName("file").hasArg().desc("Filename to grep").option("file").build());
        return options;
    }

    /**
     * Class to track the server details
     */
    public static class MachineLocation {
        private String ip;
        private int port;
        private String logFile;

        public MachineLocation() {
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public void setLogFile(String logFile) {
            this.logFile = logFile;
        }

        public String getLogFile() {
            return logFile;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }
    }
}
