package org.atlasapi.application.auth.github;

import java.io.IOException;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;

/**
 * Ask for a JSON response from GitHub
 */
public class AcceptJson implements HttpRequestInitializer {

    @Override
    public void initialize(HttpRequest request) throws IOException {
        request.setHeaders(request.getHeaders().setAccept("application/json"));
    }

}
