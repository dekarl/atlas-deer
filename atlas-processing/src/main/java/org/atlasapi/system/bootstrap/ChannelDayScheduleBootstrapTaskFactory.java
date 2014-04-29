package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.ContentStore;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.LocalDate;

public class ChannelDayScheduleBootstrapTaskFactory implements SourceChannelDayFactory<ChannelDayScheduleBootstrapTask> {

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentStore contentStore;

    public ChannelDayScheduleBootstrapTaskFactory(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentStore contentStore) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
    }

    @Override
    public ChannelDayScheduleBootstrapTask create(Publisher source, Channel channel,
            LocalDate day) {
        return new ChannelDayScheduleBootstrapTask(
                scheduleResolver,
                scheduleWriter,
                contentStore,
                channel,
                day,
                source);
    }

}
