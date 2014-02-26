package org.atlasapi.application.auth.google;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.services.oauth2.model.Userinfo;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.social.auth.credentials.AuthToken;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.user.ApplicationIdAwareUserRefBuilder;


public class GoogleAccessTokenChecker implements AccessTokenChecker {

    private final ApplicationIdAwareUserRefBuilder userRefBuilder;
    private final GoogleAuthClient googleClient;
    
    private static final Logger log = LoggerFactory.getLogger(GoogleAccessTokenChecker.class);
    
    public GoogleAccessTokenChecker(ApplicationIdAwareUserRefBuilder userRefBuilder,
            GoogleAuthClient googleClient) {
        super();
        this.userRefBuilder = checkNotNull(userRefBuilder);
        this.googleClient = checkNotNull(googleClient);
    }

    @Override
    public Maybe<UserRef> check(AuthToken accessToken) {
        checkArgument(accessToken.isFor(UserNamespace.GOOGLE));
        try {
            Userinfo user = googleClient.getUserForToken(accessToken);
            return Maybe.just(userRefBuilder.from(String.valueOf(user.getId()), UserNamespace.GOOGLE));
        } catch (IOException e) {
            Throwables.propagate(e);
            return null;
        }
    }

}
