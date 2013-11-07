package org.atlasapi.application;

import org.atlasapi.application.auth.MongoTokenRequestStore;
import org.atlasapi.application.auth.OAuthRequestListWriter;
import org.atlasapi.application.auth.OAuthRequestQueryResultWriter;
import org.atlasapi.application.auth.OAuthResultListWriter;
import org.atlasapi.application.auth.OAuthResultQueryResultWriter;
import org.atlasapi.application.auth.twitter.TwitterAuthController;
import org.atlasapi.application.model.auth.TokenRequestStore;
import org.atlasapi.application.notification.NotifierModule;
import org.atlasapi.application.users.MongoUserStore;
import org.atlasapi.application.users.NewUserSupplier;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.www.ApplicationWebModule;
import org.atlasapi.persistence.application.ApplicationPersistenceModule;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.credentials.MongoDBCredentialsStore;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.twitter.TwitterApplication;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.social.user.FixedAppIdUserRefBuilder;
import com.metabroadcast.common.social.user.TwitterOAuth1AccessTokenChecker;

@Configuration
@Import({ ApplicationPersistenceModule.class, ApplicationWebModule.class, NotifierModule.class })
public class ApplicationModule {
    private static final String APP_NAME = "atlas";
    private @Autowired @Qualifier(value = "adminMongo") DatabasedMongo adminMongo;
    private @Autowired SourceRequestStore sourceRequestStore;
    private @Autowired LegacyApplicationStore applicationStore;
    @Value("${twitter.auth.consumerKey}") private String consumerKey;
    @Value("${twitter.auth.consumerSecret}") private String consumerSecret;
    @Value("${local.host.name}") private String host;

    public @Bean
    UserStore userStore() {
        return new MongoUserStore(adminMongo, applicationStore);
    }

    public @Bean
    CredentialsStore credentialsStore() {
        return new MongoDBCredentialsStore(adminMongo);
    }
    
    public @Bean TwitterAuthController twitterAuthController() {
        TokenRequestStore tokenRequestStore = new MongoTokenRequestStore(adminMongo);
        return new TwitterAuthController(new TwitterApplication(consumerKey, consumerSecret), 
                accessTokenProcessor(),
                userStore(), 
                new NewUserSupplier(new MongoSequentialIdGenerator(adminMongo, "users")),
                tokenRequestStore,
                new OAuthRequestQueryResultWriter(new OAuthRequestListWriter()),
                new OAuthResultQueryResultWriter(new OAuthResultListWriter())
                );
    }
    
    public @Bean FixedAppIdUserRefBuilder userRefBuilder() {
        return new FixedAppIdUserRefBuilder(APP_NAME);
    }
    
    public @Bean AccessTokenChecker accessTokenChecker() {
        return new TwitterOAuth1AccessTokenChecker(userRefBuilder() , consumerKey, consumerSecret);
    }
    
    public @Bean AccessTokenProcessor accessTokenProcessor() {
        return new AccessTokenProcessor(accessTokenChecker(), credentialsStore());
    }
}
