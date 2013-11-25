package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.QueryContext;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableSet;

public abstract class ScheduleQuery {
    
    public static final ScheduleQuery single(Publisher source, Interval interval, QueryContext context, Id channelId) {
        return new SingleScheduleQuery(source, interval, context, channelId);
    }
    
    public static final ScheduleQuery multi(Publisher source, Interval interval, QueryContext context, List<Id> channelIds) {
        return new MultiScheduleQuery(source, interval, context, channelIds);
    }
    
    private final Publisher source;
    private final Interval interval;
    private final QueryContext context;

    public ScheduleQuery(Publisher source, Interval interval, QueryContext context) {
        this.source = checkNotNull(source);
        this.interval = checkNotNull(interval);
        this.context = checkNotNull(context);
    }
    
    public abstract boolean isMultiChannel();
    
    public abstract Id getChannelId();
    
    public abstract ImmutableSet<Id> getChannelIds();
    
    public Publisher getSource() {
        return source;
    }
        
    public Interval getInterval() {
        return interval;
    }
    
    public QueryContext getContext() {
        return this.context;
    }
    
    private static final class SingleScheduleQuery extends ScheduleQuery {

        private final Id channelId;

        public SingleScheduleQuery(Publisher source, Interval interval, QueryContext context, Id channelId) {
            super(source, interval, context);
            this.channelId = channelId;
        }
        
        @Override
        public boolean isMultiChannel() {
            return false;
        }

        @Override
        public Id getChannelId() {
            return channelId;
        }

        @Override
        public ImmutableSet<Id> getChannelIds() {
            throw new IllegalStateException("Can't call ScheduleQuery.getChannelIds() on single query");
        }
        
    }

    private static final class MultiScheduleQuery extends ScheduleQuery {

        private final ImmutableSet<Id> channelIds;

        public MultiScheduleQuery(Publisher source, Interval interval, QueryContext context, List<Id> ids) {
            super(source, interval, context);
            this.channelIds = ImmutableSet.copyOf(ids);
        }

        @Override
        public boolean isMultiChannel() {
            return true;
        }

        @Override
        public Id getChannelId() {
            throw new IllegalStateException("Can't call ScheduleQuery.getChannelId() on multi query");
        }

        @Override
        public ImmutableSet<Id> getChannelIds() {
            return channelIds;
        }
        
    }
    
}
