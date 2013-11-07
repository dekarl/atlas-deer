package org.atlasapi.output.useraware;

import java.io.IOException;

import org.atlasapi.output.ResponseWriter;

public interface UserAwareQueryResultWriter<T> {

    void write(UserAwareQueryResult<T> result, ResponseWriter responseWriter) throws IOException;

}