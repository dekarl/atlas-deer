package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Version;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.LocationsAnnotation.EncodedLocationWriter;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableList.Builder;

public class AvailableLocationsAnnotation extends OutputAnnotation<Content> {

    private final EncodedLocationWriter encodedLocationWriter;

    public AvailableLocationsAnnotation() {
        super();
        this.encodedLocationWriter = new EncodedLocationWriter("available_locations");        
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            writer.writeList(encodedLocationWriter, encodedLocations(item), ctxt);
        }
    }

    private Boolean isAvailable(Policy input) {
        return (input.getAvailabilityStart() == null || ! (new DateTime(input.getAvailabilityStart()).isAfterNow()))
            && (input.getAvailabilityEnd() == null || new DateTime(input.getAvailabilityEnd()).isAfterNow());
    }
    
    private Iterable<EncodedLocation> encodedLocations(Item item) {
        return Iterables.concat(Iterables.transform(item.getVersions(),
            new Function<Version, Iterable<EncodedLocation>>() {
                @Override
                public Iterable<EncodedLocation> apply(Version input) {
                    return encodedLocations(input.getManifestedAs());
                }
            }
        ));
    }

    private Iterable<EncodedLocation> encodedLocations(Set<Encoding> manifestedAs) {
        return Iterables.concat(Iterables.transform(manifestedAs,
            new Function<Encoding, Iterable<EncodedLocation>>() {
                @Override
                public Iterable<EncodedLocation> apply(Encoding encoding) {
                    Builder<EncodedLocation> builder = ImmutableList.builder();
                    for (Location location : encoding.getAvailableAt()) {
                        if (isAvailable(location.getPolicy())) {
                            builder.add(new EncodedLocation(encoding, location));
                        }
                    }
                    return builder.build();
                }
            }
        ));
    }
}
