package com.cs425;

import java.util.Arrays;

import com.cs425.grep.GrepResponse;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GrepResponseTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public GrepResponseTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(GrepResponseTest.class);
    }

    public void testInitialized() {
        String filename = "someFile";
        GrepResponse response = new GrepResponse(Arrays.asList("line1", "line2"), filename);

        assertTrue(response.isInitialized());
    }

    public void testFileNotFound() {
        String filename = "someFile";
        GrepResponse response = new GrepResponse(filename);

        assertTrue(response.isInitialized());
        assertFalse(response.fileExists());
    }

    public void testEquals() {
        GrepResponse one = new GrepResponse(Arrays.asList("line1", "line2"), "file");
        GrepResponse two = new GrepResponse(Arrays.asList("line1", "line2"), "file");

        assertTrue(one.equals(two));
        assertTrue(two.equals(one));
    }

    public void testEqualsUninitialized() {
        GrepResponse one = new GrepResponse();
        GrepResponse two = new GrepResponse();

        assertTrue(one.equals(two));
        assertTrue(two.equals(one));
    }

    public void testEqualsFileNotFound() {
        GrepResponse one = new GrepResponse("file");
        GrepResponse two = new GrepResponse("file");

        assertTrue(one.equals(two));
        assertTrue(two.equals(one));
    }

    public void testNotEqualLines() {
        GrepResponse one = new GrepResponse(Arrays.asList("line1", "line2"), "file");
        GrepResponse two = new GrepResponse(Arrays.asList("differentline1", "differentline2"), "file");

        assertFalse(one.equals(two));
        assertFalse(two.equals(one));
    }

    public void testNotEqualFile() {
        GrepResponse one = new GrepResponse(Arrays.asList("line1", "line2"), "file");
        GrepResponse two = new GrepResponse(Arrays.asList("line1", "line2"), "differentfile");

        assertFalse(one.equals(two));
        assertFalse(two.equals(one));
    }

    public void testNotEqualByConstructor() {
        GrepResponse one = new GrepResponse(Arrays.asList("line1", "line2"), "file");
        GrepResponse three = new GrepResponse("file");
        GrepResponse two = new GrepResponse();

        assertFalse(one.equals(two));
        assertFalse(one.equals(three));
        assertFalse(two.equals(one));
        assertFalse(two.equals(three));
        assertFalse(three.equals(one));
        assertFalse(three.equals(two));
    }
}
