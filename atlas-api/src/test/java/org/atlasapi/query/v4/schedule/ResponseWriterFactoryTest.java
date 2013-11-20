package org.atlasapi.query.v4.schedule;

import static org.junit.Assert.assertNotNull;

import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.net.HttpHeaders;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;


public class ResponseWriterFactoryTest {

    private final ResponseWriterFactory factory = new ResponseWriterFactory();
    
    private StubHttpServletResponse response;
    private StubHttpServletRequest request;
    
    @Before
    public void setup() {
        request = new StubHttpServletRequest();
        response = new StubHttpServletResponse();
    }
    
    @Test
    public void testGetsWriterForExtension() throws Exception {
        
        request.withRequestUri("/extension/is.json");
        
        ResponseWriter writer = factory.writerFor(request, response);
        
        assertNotNull("writer should not be null", writer);
        
    }
    
    @Test
    public void testGetsWriterForAccept() throws Exception {
        
        request.withRequestUri("/extension/is/missing");
        request.withHeader(HttpHeaders.ACCEPT, "application/json");
        
        ResponseWriter writer = factory.writerFor(request, response);
        
        assertNotNull("writer should not be null", writer);
        
    }
    
    @Test(expected=NotAcceptableException.class)
    public void testNotAcceptableForNoExtensionOrAccept() throws Exception {
        
        request.withRequestUri("/extension/is/missing");
        
        factory.writerFor(request, response);
        
    }

    @Test(expected=NotAcceptableException.class)
    public void testWritesNotAcceptableForUnknownAcceptHeader() throws Exception {
        
        request.withRequestUri("/extension/is/missing");
        request.withHeader(HttpHeaders.ACCEPT, "application/elephant");
        
        factory.writerFor(request, response);
        
    }

    @Test(expected=UnsupportedFormatException.class)
    public void testWritesNotFoundForUnknownExtension() throws Exception {
        
        request.withRequestUri("/extension/is.unknown");
        
        factory.writerFor(request, response);
        
    }

}
