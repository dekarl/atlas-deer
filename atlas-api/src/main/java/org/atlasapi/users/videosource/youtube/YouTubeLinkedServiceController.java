package org.atlasapi.users.videosource.youtube;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.users.videosource.model.OauthTokenDetails;
import org.atlasapi.users.videosource.model.UserVideoSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.api.client.auth.oauth2.AuthorizationCodeResponseUrl;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Userinfo;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

@Controller
public class YouTubeLinkedServiceController {

    private static Logger log = LoggerFactory.getLogger(YouTubeLinkedServiceController.class);
    private static final String YT_APP_NAME = "Atlas";
    private static final List<String> SCOPES_REQUIRED = Arrays.asList(
            "https://www.googleapis.com/auth/youtube.readonly",
            "https://www.googleapis.com/auth/userinfo.profile");
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final String youTubeClientId;
    private final String youTubeClientSecret;
    private final QueryResultWriter<OAuthRequest> oauthRequestResultWriter;
    private final JsonFactory jsonFactory = new JacksonFactory();
    private final HttpTransport httpTransport = new NetHttpTransport();
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;

    public YouTubeLinkedServiceController(String youTubeClientId,
            String youTubeClientSecret,
            QueryResultWriter<OAuthRequest> oauthRequestResultWriter,
            UserFetcher userFetcher,
            NumberToShortStringCodec idCodec) {
        super();
        this.youTubeClientId = youTubeClientId;
        this.youTubeClientSecret = youTubeClientSecret;
        this.oauthRequestResultWriter = oauthRequestResultWriter;
        this.userFetcher = userFetcher;
        this.idCodec = idCodec;
    }

    // Any callback URL used must be registered in the Google API console
    @RequestMapping(value = { "/4.0/videosource/youtube/login.*" }, method = RequestMethod.GET)
    public void getYouTubeLinkLogin(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(required = true) String callbackUrl) throws UnsupportedFormatException,
            NotAcceptableException, IOException {
        User user = userFetcher.userFor(request).get();
        String userId = idCodec.encode(user.getId().toBigInteger());
        ResponseWriter writer = writerResolver.writerFor(request, response);
        String authUrl = new GoogleAuthorizationCodeRequestUrl(youTubeClientId,
                callbackUrl, SCOPES_REQUIRED)
                .setAccessType("offline")
                .setState(userId)
                .setApprovalPrompt("force")
                .build();
        OAuthRequest oauthRequest = OAuthRequest.builder()
                .withNamespace(UserNamespace.YOUTUBE)
                .withAuthUrl(authUrl)
                .withToken("")
                .withSecret("")
                .build();
        QueryResult<OAuthRequest> queryResult = QueryResult.singleResult(oauthRequest,
                QueryContext.standard());
        oauthRequestResultWriter.write(queryResult, writer);
    }

    @RequestMapping(value = { "/4.0/videosource/youtube/token.*" }, method = RequestMethod.GET)
    public void getAccessToken(HttpServletResponse response, HttpServletRequest request)
            throws IOException {
        StringBuffer fullUrlBuf = request.getRequestURL();
        if (request.getQueryString() != null) {
            fullUrlBuf.append('?').append(request.getQueryString());
        }
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            AuthorizationCodeResponseUrl authResponse = new AuthorizationCodeResponseUrl(
                    fullUrlBuf.toString());
            if (authResponse.getError() != null) {
                String errorMsg = String.format("Error received from remote Oauth server: %s",
                        authResponse.getError());
                throw new NotAcceptableException(errorMsg);
            }
            GoogleTokenResponse googleTokenResponse = getGoogleTokenResponse(authResponse,
                    request.getRequestURL().toString());
            OauthTokenDetails tokenResponse = getOauthTokenFromTokenResponse(googleTokenResponse);
            // get the youtube user info
            Userinfo userinfo = getUserInfo(googleTokenResponse);
            // get the passed atlas user from the scope
            Id atlasUser = Id.valueOf(idCodec.decode(request.getParameter("state")));
            // Future ticket: user assigned publisher
            UserVideoSource userVideoSource = UserVideoSource.builder()
                    .withUserRef(new UserRef(userinfo.getId(), UserNamespace.YOUTUBE))
                    .withAtlasUser(atlasUser)
                    .withName(userinfo.getName())
                    .withPublisher(Publisher.YOUTUBE)
                    .build();
            // Future ticket: store

        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private Credential getCredentialForToken(GoogleTokenResponse tokenResponse) {
        return new GoogleCredential.Builder()
                .setJsonFactory(jsonFactory)
                .setTransport(httpTransport)
                .setClientSecrets(youTubeClientId, youTubeClientSecret).build()
                .setFromTokenResponse(tokenResponse);
    }

    private GoogleTokenResponse getGoogleTokenResponse(AuthorizationCodeResponseUrl authResponse,
            String redirectUri) throws IOException {
        return new GoogleAuthorizationCodeTokenRequest(httpTransport,
                jsonFactory,
                youTubeClientId,
                youTubeClientSecret,
                authResponse.getCode(),
                redirectUri)
                .execute();
    }

    private OauthTokenDetails getOauthTokenFromTokenResponse(GoogleTokenResponse tokenResponse) {
        return OauthTokenDetails.builder()
                .withAccessToken(tokenResponse.getAccessToken())
                .withTokenType(tokenResponse.getTokenType())
                .withExpiresIn(tokenResponse.getExpiresInSeconds())
                .withIdToken(tokenResponse.getIdToken())
                .withRefreshToken(tokenResponse.getRefreshToken())
                .build();
    }

    private Userinfo getUserInfo(GoogleTokenResponse tokenResponse) throws IOException {
        Credential credential = getCredentialForToken(tokenResponse);
        Oauth2 oauth2 = new Oauth2.Builder(httpTransport, jsonFactory, credential).setApplicationName(
                YT_APP_NAME)
                .build();
        return oauth2.userinfo().get().execute();
    }
}
