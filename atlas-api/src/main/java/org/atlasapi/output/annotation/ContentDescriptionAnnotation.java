package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.content.ContentVisitor;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Song;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemDisplayTitleWriter;
import org.joda.time.Duration;

public class ContentDescriptionAnnotation extends DescriptionAnnotation<Content> {

    private final ItemDisplayTitleWriter displayTitleWriter = new ItemDisplayTitleWriter();

    public ContentDescriptionAnnotation() {
    }

    @Override
    public void write(Content content, final FieldWriter writer, final OutputContext ctxt) throws IOException {
        content.accept(new ContentVisitor<Void>(){

            public void writeField(String field, Object value) {
                try {
                    writer.writeField(field, value);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }

            public <T> void writeObject(EntityWriter<T> field, T value, OutputContext ctxt) {
                try {
                    writer.writeObject(field, value, ctxt);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
            
            @Override
            public Void visit(Brand brand) {
                return null;
            }

            @Override
            public Void visit(Series series) {
                writeField("series_number", series.getSeriesNumber());
                writeField("total_episodes", series.getTotalEpisodes());
                return null;
            }

            @Override
            public Void visit(Episode episode) {
                visit((Item) episode);
                writeField("episode_number", episode.getEpisodeNumber());
                writeField("series_number", episode.getSeriesNumber());
                return null;
            }

            @Override
            public Void visit(Film film) {
                visit((Item) film);
                writeField("year", film.getYear());
                return null;
            }

            @Override
            public Void visit(Song song) {
                visit((Item) song);
                writeField("isrc", song.getIsrc());
                Duration duration = song.getDuration();
                writeField("duration", duration != null ? duration.getStandardSeconds()
                                                        : null);
                return null;
            }

            @Override
            public Void visit(Item item) {
                writeObject(displayTitleWriter, item, ctxt);
                return null;
            }

            @Override
            public Void visit(Clip clip) {
                return null;
            }
        });
        
        writer.writeField("media_type", content.getMediaType());
        writer.writeField("specialization", content.getSpecialization());
        
        super.write(content, writer, ctxt);
    }

}
