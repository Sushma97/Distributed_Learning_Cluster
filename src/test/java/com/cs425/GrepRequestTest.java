package com.cs425;

import java.net.URISyntaxException;
import java.util.*;

import com.cs425.grep.Client;
import com.cs425.grep.ClientThread;
import com.cs425.grep.GrepRequest;
import com.cs425.grep.GrepResponse;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GrepRequestTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public GrepRequestTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(GrepRequestTest.class);
    }

    public void testGetFileName() {
        String filename = "someFile";
        GrepRequest request = new GrepRequest("pattern", filename, null);

        assertEquals(filename, request.getFilename());
    }

    public void testGetGrepPattern() {
        String pattern = "somePattern";
        GrepRequest request = new GrepRequest(pattern, "someFile", Arrays.asList("c"));

        assertEquals(pattern, request.getGrepPattern());
    }

    public void testToString() {
        GrepRequest request = new GrepRequest("pattern", "file", Arrays.asList("c"));

        assertEquals(request.toString(),
        "GrepRequest{filename='file', grepPattern='pattern'}");
    }

    public void testEqualsBothNull() {
        GrepRequest one = new GrepRequest(null, null, null);
        GrepRequest two = new GrepRequest(null, null, null);

        assertTrue(one.equals(two));
    }

    public void testEquals() {
        GrepRequest one = new GrepRequest("pattern", "file", Arrays.asList("c"));
        GrepRequest two = new GrepRequest("pattern", "file", Arrays.asList("c"));

        assertTrue(one.equals(two));
    }

    public void testNotEqualsPattern() {
        GrepRequest one = new GrepRequest("pattern", "file", Arrays.asList("c"));
        GrepRequest two = new GrepRequest("differentpattern", "file", Arrays.asList("c"));

        assertFalse(one.equals(two));
    }

    public void testNotEqualsFile() {
        GrepRequest one = new GrepRequest("pattern", "file", Arrays.asList("c"));
        GrepRequest two = new GrepRequest("pattern", "differentfile", Arrays.asList("c"));

        assertFalse(one.equals(two));
    }

    public void testNotEqualsOptions() {
        GrepRequest one = new GrepRequest("pattern", "file", Arrays.asList("c"));
        GrepRequest two = new GrepRequest("pattern", "file", Arrays.asList("n"));

        assertFalse(one.equals(two));
    }

    public void testNotEqualsOneNull() {
        GrepRequest one = new GrepRequest(null, null, null);
        GrepRequest two = new GrepRequest("pattern", "file", Arrays.asList("c"));

        assertFalse(one.equals(two));
        assertFalse(two.equals(one));
    }

    public void testRunGrepInfrequentLines() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        String path = classLoader.getResource("result.txt").getPath();
        
        GrepRequest request = new GrepRequest("Sherlock", path, Arrays.asList("c"));
        GrepResponse response = request.runGrep();

        assertTrue("Response not initialized", response.isInitialized());
        assertTrue("File not found", response.fileExists());

        String count = response.lines.get(0);

        assertEquals(count, "13");
    }

    public void testRunGrepFrequentLines() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        String path = classLoader.getResource("result.txt").getPath();
        
        GrepRequest request = new GrepRequest("a", path, Arrays.asList("c"));
        GrepResponse response = request.runGrep();

        assertTrue("Response not initialized", response.isInitialized());
        assertTrue("File not found", response.fileExists());

        String count = response.lines.get(0);

        assertEquals(count, "187");
    }

    public void testRunGrepLoadTestInfrequent() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        String path = classLoader.getResource("enwik8").getPath();
        
        GrepRequest request = new GrepRequest("hello", path, null);
        GrepResponse response = request.runGrep();

        assertTrue("Response not initialized", response.isInitialized());
        assertTrue("File not found", response.fileExists());

        assertEquals(153, response.lines.size());
    }

    public void testRunGrepLoadTestFrequent() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        String path = classLoader.getResource("enwik8").getPath();
        
        GrepRequest request = new GrepRequest("e", path, null);
        GrepResponse response = request.runGrep();

        assertTrue("Response not initialized", response.isInitialized());
        assertTrue("File not found", response.fileExists());

        assertEquals(693181, response.lines.size());
    }

    static Map<String, int[]> expectedResults;
    static {
        expectedResults = new HashMap<>();
        expectedResults.put("hello", new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        expectedResults.put("10/Feb/2024", new int[]{145, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        expectedResults.put("DELETE", new int[]{27976, 26754, 26854, 27144, 27293, 26846, 27031, 27294, 26898, 26437});
        expectedResults.put(".com", new int[]{176215, 166375, 166628, 167602, 168172, 166856, 166647, 170350, 167511, 164903});
    }

    public void testRunGrepDistributed() {
        // To keep track of offline machines, so we skip testing those
        Set<Integer> offlineMachines = new HashSet<>();
        List<Client.MachineLocation> list = Client.getServerList("server.json");
        for (Map.Entry<String,int[]> entry : expectedResults.entrySet()) {
            String pattern = entry.getKey();
            int[] expectedValues = entry.getValue();

            for (int i = 0; i < expectedValues.length; ++i) {
                // Skip this machine if it is offline
                if (offlineMachines.contains(i)) {
                    continue;
                }

                // Create request and thread
                GrepRequest request = new GrepRequest(pattern, list.get(i).getLogFile(), Arrays.asList("c"));
                ClientThread thread = new ClientThread(list.get(i).getIp(), list.get(i).getPort(), request, null);

                // Run request asynchronously
                thread.start();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Thread interrupted");
                }
                GrepResponse response = thread.getGrepResponse();

                // If response is null, there was an error with thread creation/running
                assertNotNull(response);

                // Only check equal if server was online
                if (response.isInitialized()) {
                    assertEquals(expectedValues[i], Integer.parseInt(response.lines.get(0)));
                } else {
                    // Mark machine as offline so we skip later
                    offlineMachines.add(i);
                }
            }
        }
    }

}
