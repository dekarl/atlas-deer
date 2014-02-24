package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.ContentResolver;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.LocalDate;

public class ChannelDayScheduleBootstrapTaskFactory implements SourceChannelDayFactory<ChannelDayScheduleBootstrapTask> {

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentResolver contentResolver;

    public ChannelDayScheduleBootstrapTaskFactory(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentResolver contentResolver) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override
    public ChannelDayScheduleBootstrapTask create(Publisher source, Channel channel,
            LocalDate day) {
        return new ChannelDayScheduleBootstrapTask(
                scheduleResolver,
                scheduleWriter,
                contentResolver,
                channel,
                day,
                source);
    }

}
