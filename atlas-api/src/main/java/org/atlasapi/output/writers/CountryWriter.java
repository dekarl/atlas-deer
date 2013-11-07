package org.atlasapi.output.writers;

import java.io.IOException;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.intl.Country;


public class CountryWriter implements EntityWriter<Country> {

    @Override
    public void write(Country entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("code", entity.code());
        writer.writeField("name", entity.getName());
    }

    @Override
    public String fieldName(Country entity) {
        return "country";
    }

}
