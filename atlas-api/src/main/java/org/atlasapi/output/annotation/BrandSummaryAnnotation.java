package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Item.ContainerSummary;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.persistence.output.ContainerSummaryResolver;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class BrandSummaryAnnotation extends OutputAnnotation<Content> {

    private static final class ContainerSummaryWriter implements EntityWriter<Item> {

        private final ContainerSummaryResolver containerSummaryResolver;
        private final NumberToShortStringCodec idCodec;

        public ContainerSummaryWriter(NumberToShortStringCodec idCodec, ContainerSummaryResolver containerSummaryResolver) {
            this.idCodec = checkNotNull(idCodec);
            this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
        }

        @Override
        public void write(Item entity, FieldWriter writer, OutputContext ctxt) throws IOException {
            ParentRef container = entity.getContainer();
            ContainerSummary summary = entity.getContainerSummary();
            writer.writeField("id", idCodec.encode(container.getId().toBigInteger()));
            if (summary != null) {
                writer.writeField("type", summary.getType());
                writer.writeField("title", summary.getTitle());
                writer.writeField("description", summary.getDescription());
                if (summary.getSeriesNumber() != null) {
                    writer.writeField("series_number", summary.getSeriesNumber());
                }
            } 
        }

        @Override
        public String fieldName(Item entity) {
            return "container";
        }
    }

    private final ContainerSummaryWriter summaryWriter;

    public BrandSummaryAnnotation(NumberToShortStringCodec idCodec, ContainerSummaryResolver containerSummaryResolver) {
        summaryWriter = new ContainerSummaryWriter(idCodec, containerSummaryResolver);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            if (item.getContainer() == null) {
                writer.writeField("container", null);
            } else {
                writer.writeObject(summaryWriter, item, ctxt);
            }
        }
    }

}
