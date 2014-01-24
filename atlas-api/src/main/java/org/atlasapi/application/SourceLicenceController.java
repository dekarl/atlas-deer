package org.atlasapi.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class SourceLicenceController {
    private static Logger log = LoggerFactory.getLogger(SourceLicenceController.class);
    private final UserAwareQueryParser<SourceLicence> queryParser;
    private final UserAwareQueryExecutor<SourceLicence> queryExecutor;
    private final UserAwareQueryResultWriter<SourceLicence> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final ModelReader reader;
    private final UserFetcher userFetcher;
    private final SourceLicenceStore store;
    
    public SourceLicenceController(UserAwareQueryParser<SourceLicence> queryParser,
            UserAwareQueryExecutor<SourceLicence> queryExecutor,
            UserAwareQueryResultWriter<SourceLicence> resultWriter,
            ModelReader reader,
            UserFetcher userFetcher,
            SourceLicenceStore store) {
        super();
        this.queryParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.userFetcher = userFetcher;
        this.store = store;
    }
    
    // TODO request multiple licences?
    @RequestMapping({"/4.0/source_licences/{sid}.*", "/4.0/source_licences.*" })
    public void listSources(HttpServletRequest request,
            HttpServletResponse response) throws QueryParseException, QueryExecutionException, IOException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            UserAwareQuery<SourceLicence> sourcesQuery = queryParser.parse(request);
            UserAwareQueryResult<SourceLicence> queryResult = queryExecutor.execute(sourcesQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    @RequestMapping(value = "/4.0/licence.*", method = RequestMethod.POST)
    public void writeLicence(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            User user = userFetcher.userFor(request).get();
            if (!user.is(Role.ADMIN)) {
                throw new ResourceForbiddenException();
            }
           
            SourceLicence licence = deserialize(new InputStreamReader(request.getInputStream()), SourceLicence.class);
            store.store(licence);
            UserAwareQueryResult<SourceLicence> queryResult = UserAwareQueryResult.singleResult(licence, UserAwareQueryContext.standard());
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }
}
