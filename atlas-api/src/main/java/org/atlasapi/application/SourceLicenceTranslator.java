package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class SourceLicenceTranslator {
    public static final String SOURCE_KEY = MongoConstants.ID;
    public static final String LICENCE_KEY = "licence";
    
    public DBObject toDBObject(SourceLicence sourceLicence) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, SOURCE_KEY, sourceLicence.getSource().key());
        TranslatorUtils.from(dbo, LICENCE_KEY, sourceLicence.getLicence());
        return dbo;
    }
    
    public SourceLicence fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return SourceLicence.builder()
                .withSource(Publisher.fromKey(TranslatorUtils.toString(dbo, SOURCE_KEY)).requireValue())
                .withLicence(TranslatorUtils.toString(dbo, LICENCE_KEY))
                .build();
    }
}
