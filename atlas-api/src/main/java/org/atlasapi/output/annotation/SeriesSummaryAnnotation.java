package org.atlasapi.output.annotation;


import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.persistence.output.ContainerSummaryResolver;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class SeriesSummaryAnnotation extends OutputAnnotation<Content> {

    private final SeriesSummaryWriter summaryWriter;

    private final class SeriesSummaryWriter implements EntityWriter<Episode> {

        private final ContainerSummaryResolver containerSummaryResolver;
        private final NumberToShortStringCodec idCodec;
        
        
        public SeriesSummaryWriter(NumberToShortStringCodec idCodec, ContainerSummaryResolver containerSummaryResolver) {
            this.idCodec = checkNotNull(idCodec);
            this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
        }

        @Override
        public void write(Episode entity, FieldWriter writer, OutputContext ctxt) throws IOException {
            writer.writeField("id", idCodec.encode(entity.getSeriesRef().getId().toBigInteger()));
           
        }

        @Override
        public String fieldName(Episode entity) {
            return "series";
        }
    }

    public SeriesSummaryAnnotation(NumberToShortStringCodec idCodec, ContainerSummaryResolver containerSummaryResolver) {
        summaryWriter = new SeriesSummaryWriter(idCodec, containerSummaryResolver);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Episode) {
            Episode episode = (Episode) entity;
            if (episode.getSeriesRef() == null) {
                writer.writeField("series", null);
            } else {
                writer.writeObject(summaryWriter, episode, ctxt);
            }
        }
    }

}
