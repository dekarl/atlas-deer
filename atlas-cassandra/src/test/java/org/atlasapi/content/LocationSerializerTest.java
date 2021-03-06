package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Currency;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;


public class LocationSerializerTest {

    private final LocationSerializer serializer = new LocationSerializer();
    
    @Test
    public void testDeSerializeLocation() throws Exception {
        Location location = new Location();
        location.setEmbedCode("embedCode");
        location.setEmbedId("embedid");
        location.setTransportIsLive(true);
        location.setTransportSubType(TransportSubType.ITUNES);
        location.setTransportType(TransportType.LINK);
        location.setUri("uri");
        
        Policy policy = new Policy();
        policy.setActualAvailabilityStart(new DateTime(DateTimeZones.UTC));
        policy.setAvailabilityStart(new DateTime(DateTimeZones.UTC));
        policy.setAvailabilityLength(1234);
        policy.setAvailabilityEnd(new DateTime(DateTimeZones.UTC));
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setDrmPlayableFrom(new DateTime(DateTimeZones.UTC));
        policy.setNetwork(Policy.Network.WIFI);
        policy.setPlatform(Policy.Platform.IOS);
        policy.setPrice(new Price(Currency.getInstance("GBP"), 400));
        policy.setRevenueContract(Policy.RevenueContract.PAY_TO_BUY);
        
        location.setPolicy(policy);
        
        byte[] bytes = serializer.serialize(location).build().toByteArray();
        
        Location deserialized = serializer.deserialize(ContentProtos.Location.parseFrom(bytes));
        
        assertThat(deserialized.getEmbedCode(), is(location.getEmbedCode()));
        assertThat(deserialized.getEmbedId(), is(location.getEmbedId()));
        assertThat(deserialized.getTransportIsLive(), is(location.getTransportIsLive()));
        assertThat(deserialized.getTransportSubType(), is(location.getTransportSubType()));
        assertThat(deserialized.getTransportType(), is(location.getTransportType()));
        assertThat(deserialized.getUri(), is(location.getUri()));
        
        Policy deserializedPolicy = deserialized.getPolicy();
        
        assertThat(deserializedPolicy.getActualAvailabilityStart(), is(policy.getActualAvailabilityStart()));
        assertThat(deserializedPolicy.getAvailabilityStart(), is(policy.getAvailabilityStart()));
        assertThat(deserializedPolicy.getAvailabilityLength(), is(policy.getAvailabilityLength()));
        assertThat(deserializedPolicy.getAvailabilityEnd(), is(policy.getAvailabilityEnd()));
        assertThat(deserializedPolicy.getAvailableCountries(), is(policy.getAvailableCountries()));
        assertThat(deserializedPolicy.getDrmPlayableFrom(), is(policy.getDrmPlayableFrom()));
        assertThat(deserializedPolicy.getNetwork(), is(policy.getNetwork()));
        assertThat(deserializedPolicy.getPlatform(), is(policy.getPlatform()));
        assertThat(deserializedPolicy.getPrice(), is(policy.getPrice()));
        assertThat(deserializedPolicy.getRevenueContract(), is(policy.getRevenueContract()));
        
    }

}
