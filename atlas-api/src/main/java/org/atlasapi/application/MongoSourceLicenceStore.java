package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;


public class MongoSourceLicenceStore implements SourceLicenceStore {
    public static final String SOURCE_LICENCES_COLLECTION = "sourceLicences";
    
    private SourceLicenceTranslator translator = new SourceLicenceTranslator();
    
    private final DBCollection sourceLicences;
    
    public MongoSourceLicenceStore(DatabasedMongo mongo) {
        this.sourceLicences = mongo.collection(SOURCE_LICENCES_COLLECTION);
    }

    @Override
    public Optional<SourceLicence> licenceFor(Publisher source) {
        return Optional.fromNullable(
                translator.fromDBObject(sourceLicences.findOne(where().idEquals(source.key()).build())));
    }

    @Override
    public void store(SourceLicence sourceLicence) {
        sourceLicences.save(translator.toDBObject(sourceLicence));
    }

}
