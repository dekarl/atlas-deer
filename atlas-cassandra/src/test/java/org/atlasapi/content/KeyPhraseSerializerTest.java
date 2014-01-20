package org.atlasapi.content;

import static org.testng.AssertJUnit.assertNull;
import static org.junit.Assert.assertThat;
import org.testng.annotations.Test;
import static org.hamcrest.Matchers.is;


public class KeyPhraseSerializerTest {

    private final KeyPhraseSerializer serializer = new KeyPhraseSerializer();
    
    @Test
    public void testDeSerializingUnweightedKeyPhrase() {
        KeyPhrase phrase = new KeyPhrase("phrase",null);
        KeyPhrase deserialized = serializer.deserialize(serializer.serialize(phrase));
        assertThat(deserialized.getPhrase(), is(phrase.getPhrase()));
        assertNull(deserialized.getWeighting());
    }

    @Test
    public void testDeSerializingWeightedKeyPhrase() {
        KeyPhrase phrase = new KeyPhrase("phrase",1.0);
        KeyPhrase deserialized = serializer.deserialize(serializer.serialize(phrase));
        assertThat(deserialized.getPhrase(), is(phrase.getPhrase()));
        assertThat(deserialized.getWeighting(), is(phrase.getWeighting()));
    }

}
