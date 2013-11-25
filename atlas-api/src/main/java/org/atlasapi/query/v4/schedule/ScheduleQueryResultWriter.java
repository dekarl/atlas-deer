package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;

import com.google.common.collect.FluentIterable;

public class ScheduleQueryResultWriter implements QueryResultWriter<ChannelSchedule> {

    private final EntityListWriter<ChannelSchedule> scheduleWriter;

    public ScheduleQueryResultWriter(EntityListWriter<ChannelSchedule> scheduleWriter) {
        this.scheduleWriter = scheduleWriter;
    }

    @Override
    public void write(QueryResult<ChannelSchedule> result, ResponseWriter writer) throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(QueryResult<ChannelSchedule> result, ResponseWriter writer)
        throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        //TODO: train-wreck
        if (result.getContext().getAnnotations() == ActiveAnnotations.standard()) {
            writer.writeField("license",
                "In accessing this feed, you agree that you will only " +
                "access its contents for your own personal and non-commercial " +
                "use, and not for any commercial or other purposes, including " +
                "but not restricted to advertising or selling any goods or " +
                "services, including any third-party software applications " +
                "available to the general public.");
        }
        if (result.isListResult()) {
            FluentIterable<ChannelSchedule> resources = result.getResources();
            writer.writeList(scheduleWriter, resources, ctxt);
        } else {
            writer.writeObject(scheduleWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(QueryContext queryContext) {
        return new OutputContext(
            queryContext.getAnnotations(),
            queryContext.getApplicationSources()
        );
    }

}
