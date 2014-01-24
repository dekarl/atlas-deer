package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceLicence;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class SourceLicenceWithIdWriter implements EntityListWriter<SourceLicence> {
    private final EntityWriter<Publisher> sourceWriter;

    public SourceLicenceWithIdWriter(SourceIdCodec sourceIdCodec) {
        this.sourceWriter =  new SourceWithIdWriter(sourceIdCodec, "source", "source");
    }

    @Override
    public void write(SourceLicence entity, FieldWriter writer,
            OutputContext ctxt) throws IOException {
        writer.writeObject(sourceWriter, entity.getSource(), ctxt);
        writer.writeField("licence", entity.getLicence());
    }

    @Override
    public String fieldName(SourceLicence entity) {
        return "source_licence";
    }

    @Override
    public String listName() {
        return "source_licences";
    }

}
