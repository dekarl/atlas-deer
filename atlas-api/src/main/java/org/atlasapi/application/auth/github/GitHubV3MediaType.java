package org.atlasapi.application.auth.github;

import java.io.IOException;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;

/**
 * Ask for a GitHUb v3 API response
 */
public class GitHubV3MediaType implements HttpRequestInitializer {

    @Override
    public void initialize(HttpRequest request) throws IOException {
        request.setHeaders(request.getHeaders().setAccept("application/vnd.github.v3+json"));
    }

}
