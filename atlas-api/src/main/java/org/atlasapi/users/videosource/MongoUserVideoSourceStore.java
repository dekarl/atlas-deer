package org.atlasapi.users.videosource;

import org.atlasapi.users.videosource.model.UserVideoSource;
import org.atlasapi.users.videosource.model.translators.UserVideoSourceTranslator;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoUserVideoSourceStore implements
        UserVideoSourceStore {

    private static final String COLLECTION_NAME = "userVideoSources";
    private final DBCollection linkedTokens;
    private UserVideoSourceTranslator translator = new UserVideoSourceTranslator();

    public MongoUserVideoSourceStore(DatabasedMongo mongo) {
        this.linkedTokens = mongo.collection(COLLECTION_NAME);
    }

    @Override
    public void store(UserVideoSource userVideoSource) {
        linkedTokens.save(translator.toDBObject(userVideoSource));
    }

}
