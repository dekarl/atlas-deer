package org.atlasapi.application;

import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;


public class MongoSourceLicenceStoreTest {
    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    @Test
    public void test() {
        SourceLicenceStore store = new MongoSourceLicenceStore(mongo);
        SourceLicence licence = SourceLicence.builder()
                .withSource(Publisher.BBC)
                .withLicence("A licence")
                .build();
        store.store(licence);
        SourceLicence retrieved = store.licenceFor(Publisher.BBC).get();
        assertEquals(licence.getSource(), retrieved.getSource());
        assertEquals(licence.getLicence(), retrieved.getLicence());
    }

}
