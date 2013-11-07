package org.atlasapi.application.auth.www;

import static org.atlasapi.application.auth.OAuthTokenUserFetcher.OAUTH_PROVIDER_QUERY_PARAMETER;
import static org.atlasapi.application.auth.OAuthTokenUserFetcher.OAUTH_TOKEN_QUERY_PARAMETER;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.url.Urls;

@Controller
public class AuthController {
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private static Logger log = LoggerFactory.getLogger(AuthController.class);
    private final QueryResultWriter<OAuthProvider> resultWriter;
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;
    private final String USER_URL = "/4.0/users/%s.%s";
    
    public AuthController(QueryResultWriter<OAuthProvider> resultWriter,
            UserFetcher userFetcher,
            NumberToShortStringCodec idCodec) {
        this.resultWriter = resultWriter;
        this.userFetcher = userFetcher;
        this.idCodec = idCodec;
    }
    
    @RequestMapping(value = { "/4.0/auth/providers.*" }, method = RequestMethod.GET) 
    public void listAuthProviders(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            QueryResult<OAuthProvider> queryResult = QueryResult.listResult(OAuthProvider.all(), QueryContext.standard());
            resultWriter.write(queryResult, writer);
        }  catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    @RequestMapping(value = { "/4.0/auth/user.json" }, method = RequestMethod.GET) 
    public void redirectToCurrentUser(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam (value = OAUTH_PROVIDER_QUERY_PARAMETER) String oauthProvider,
            @RequestParam (value = OAUTH_TOKEN_QUERY_PARAMETER) String oauthToken) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        ResponseWriter writer = null;
        try {
            // Should always be able to get a user ref here as
            // missing oauth user would be intercepted
            Optional<User> user = userFetcher.userFor(request);
            String userUrl = String.format(USER_URL, 
                    idCodec.encode(BigInteger.valueOf(user.get().getId().longValue())),
                    "json");
            Map<String, String> oauthParams = Maps.newHashMap();
            oauthParams.put(OAUTH_PROVIDER_QUERY_PARAMETER, oauthProvider);
            oauthParams.put(OAUTH_TOKEN_QUERY_PARAMETER, oauthToken);
            response.setStatus(HttpServletResponse.SC_FOUND);
            response.sendRedirect(Urls.appendParameters(userUrl, oauthParams));
        }  catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
