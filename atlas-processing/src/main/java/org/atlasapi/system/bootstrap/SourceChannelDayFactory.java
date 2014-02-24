package org.atlasapi.system.bootstrap;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;

public interface SourceChannelDayFactory<T> {

    T create(Publisher source, Channel channel, LocalDate day);

}
