package com.cs425;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import com.cs425.grep.GrepRequest;
import com.cs425.grep.GrepResponse;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CommunicationTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CommunicationTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( CommunicationTest.class );
    }

    public void testGrepRequest() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(outstream);

        GrepRequest request = new GrepRequest("some pattern", "some file", null);

        output.writeObject(request);
        output.flush();
        byte[] bytes = outstream.toByteArray();

        ByteArrayInputStream instream = new ByteArrayInputStream(bytes);
        ObjectInputStream input = new ObjectInputStream(instream);

        GrepRequest received = (GrepRequest) input.readObject();

        assertTrue(request.equals(received));
    }

    public void testGrepResponse() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(outstream);

        GrepResponse response = new GrepResponse(Arrays.asList("line one", "line 2"), "some file");

        output.writeObject(response);
        output.flush();
        byte[] bytes = outstream.toByteArray();

        ByteArrayInputStream instream = new ByteArrayInputStream(bytes);
        ObjectInputStream input = new ObjectInputStream(instream);

        GrepResponse received = (GrepResponse) input.readObject();

        assertTrue(response.equals(received));
    }
}
