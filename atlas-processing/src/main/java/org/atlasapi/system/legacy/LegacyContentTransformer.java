package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ChildRef;
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.EntityType;
import org.atlasapi.content.Location;
import org.atlasapi.content.ParentRef;
import org.atlasapi.content.Policy;
import org.atlasapi.content.ReleaseDate.ReleaseType;
import org.atlasapi.content.Restriction;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.DateTimeZones;

public class LegacyContentTransformer extends DescribedLegacyResourceTransformer<Content, org.atlasapi.content.Content> {

    @Override
    protected org.atlasapi.content.Content createDescribed(Content input) {
        org.atlasapi.content.Content c = null;
        if (input instanceof Episode) {
            c = createEpisode((Episode)input);
        } else if (input instanceof Film) {
            c = createFilm((Film)input);
        } else if (input instanceof Song) {
            c = createSong((Song)input);
        } else if (input instanceof Clip) {
            c = createClip((Clip)input);
        } else if (input instanceof Item) {
            c = createItem((Item)input);
        } else if (input instanceof Brand) {
            c = createBrand((Brand)input);
        } else if (input instanceof Series) {
            c = createSeries((Series)input);
        } else if (input instanceof Container) {
            c = createBrand((Container)input);
        }
        return setContentFields(c);
    }

    private org.atlasapi.content.Content createBrand(Container input) {
        org.atlasapi.content.Brand b = new org.atlasapi.content.Brand();
        return setContainerFields(b, input);
    }

    private org.atlasapi.content.Content createSeries(Series input) {
        org.atlasapi.content.Series series = new org.atlasapi.content.Series();
        series.withSeriesNumber(input.getSeriesNumber());
        series.setTotalEpisodes(input.getTotalEpisodes());
        if (input.getParent() != null) {
            series.setParentRef(transformParentRef(input.getParent(), EntityType.BRAND));
        }
        return setContainerFields(series, input);
    }

    private org.atlasapi.content.Content createBrand(Brand input) {
        org.atlasapi.content.Brand b = new org.atlasapi.content.Brand();
        b.setSeriesRefs(Iterables.transform(input.getSeriesRefs(),
            new Function<org.atlasapi.media.entity.SeriesRef, SeriesRef>() {
                @Override
                public SeriesRef apply(org.atlasapi.media.entity.SeriesRef input) {
                    return new SeriesRef(Id.valueOf(input.getId()), input.getTitle(), input.getSeriesNumber(), input.getUpdated());
                }
            }
        ));
        return setContainerFields(b, input);
    }

    private <C extends org.atlasapi.content.Container> C setContainerFields(C c, Container input) {
        c.setChildRefs(Iterables.transform(input.getChildRefs(),
            new Function<org.atlasapi.media.entity.ChildRef, ChildRef>() {
                @Override
                public ChildRef apply(org.atlasapi.media.entity.ChildRef input) {
                    EntityType type = transformEnum(input.getType(), org.atlasapi.content.EntityType.class);
                    DateTime updated = Objects.firstNonNull(input.getUpdated(),new DateTime(DateTimeZones.UTC));
                    return new ChildRef(input.getId(), input.getSortKey(), updated, type);
                }
            }
        ));
        return c;
    }

    private org.atlasapi.content.Content createClip(Clip input) {
        org.atlasapi.content.Clip c= new org.atlasapi.content.Clip();
        return setItemFields(c, input);
    }

    private org.atlasapi.content.Content createItem(Item input) {
        return setItemFields(new org.atlasapi.content.Item(), input);
    }

    private <I extends org.atlasapi.content.Item> I setItemFields(I i, Item input) {
        if (input.getContainer() != null) {
            i.setParentRef(transformParentRef(input.getContainer(), EntityType.BRAND));
        }
        i.setVersions(transformVersions(input.getVersions()));
        i.setIsLongForm(input.getIsLongForm());
        i.setBlackAndWhite(input.getBlackAndWhite());
        i.setCountriesOfOrigin(input.getCountriesOfOrigin());
        i.withSortKey(input.sortKey());
        return i;
    }

    private Set<Version> transformVersions(Set<org.atlasapi.media.entity.Version> versions) {
        return ImmutableSet.copyOf(Iterables.transform(versions,
            new Function<org.atlasapi.media.entity.Version, Version>() {
                @Override
                public Version apply(org.atlasapi.media.entity.Version input) {
                    return transformVersion(input);
                }

            }
        ));
    }

    private Version transformVersion(org.atlasapi.media.entity.Version input) {
        Version v = new Version();
        setIdentifiedFields(v, input);
        
        v.setPublishedDuration(input.getPublishedDuration());
        if (input.getDuration() != null) {
            v.setDuration(Duration.standardSeconds(input.getDuration()));
        }
        v.setProvider(input.getProvider());
        v.setRestriction(transformRestriction(input.getRestriction()));
        v.set3d(input.is3d());
        v.setManifestedAs(ImmutableSet.copyOf(Iterables.transform(input.getManifestedAs(),
            new Function<org.atlasapi.media.entity.Encoding, Encoding>() {
                @Override
                public Encoding apply(org.atlasapi.media.entity.Encoding input) {
                    return transformEncoding(input);
                }
            }
        )));
        v.setBroadcasts(ImmutableSet.copyOf(Iterables.transform(broadcastsWithIds(input), 
            new Function<org.atlasapi.media.entity.Broadcast, Broadcast>(){
                @Override
                public Broadcast apply(org.atlasapi.media.entity.Broadcast input) {
                    return transformBroadcast(input);
                }
            }
        )));
        v.setSegmentEvents(Iterables.transform(input.getSegmentEvents(), 
            new Function<org.atlasapi.media.segment.SegmentEvent, SegmentEvent>(){
                @Override
                public SegmentEvent apply(org.atlasapi.media.segment.SegmentEvent input) {
                    return transformSegmentEvent(input);
                }

            }
        ));
        return v;
    }

    private Set<org.atlasapi.media.entity.Broadcast> broadcastsWithIds(
            org.atlasapi.media.entity.Version input) {
        return Sets.filter(input.getBroadcasts(),
            new Predicate<org.atlasapi.media.entity.Broadcast>() {
                @Override
                public boolean apply(org.atlasapi.media.entity.Broadcast input) {
                    return input.getSourceId() != null;
                }
            }
        );
    }

    private SegmentEvent transformSegmentEvent(org.atlasapi.media.segment.SegmentEvent input) {
        SegmentEvent se = new SegmentEvent();
        setIdentifiedFields(se, input);
        se.setOffset(input.getOffset());
        se.setIsChapter(input.getIsChapter());
        org.atlasapi.media.entity.Description d = input.getDescription();
        se.setDescription(new Description(d.getTitle(), d.getSynopsis(), d.getImage(), d.getThumbnail()));
        org.atlasapi.media.segment.SegmentRef sr = input.getSegment();
        se.setSegment(new SegmentRef(sr.identifier()));
        return se;
    }

    private Encoding transformEncoding(org.atlasapi.media.entity.Encoding input) {
        Encoding e = new Encoding();
        setIdentifiedFields(e, input);
        e.setAvailableAt(transformLocations(input));
        e.setContainsAdvertising(input.getContainsAdvertising());
        e.setAdvertisingDuration(input.getAdvertisingDuration());
        e.setBitRate(input.getBitRate());
        e.setAudioBitRate(input.getAudioBitRate());
        e.setAudioChannels(input.getAudioChannels());
        e.setAudioCoding(input.getAudioCoding());
        e.setVideoAspectRatio(input.getVideoAspectRatio());
        e.setVideoBitRate(input.getVideoBitRate());
        e.setVideoCoding(input.getVideoCoding());
        e.setVideoFrameRate(input.getVideoFrameRate());
        e.setVideoHorizontalSize(input.getVideoHorizontalSize());
        e.setVideoProgressiveScan(input.getVideoProgressiveScan());
        e.setVideoVerticalSize(input.getVideoVerticalSize());
        e.setDataSize(input.getDataSize());
        e.setDataContainerFormat(input.getDataContainerFormat());
        e.setSource(input.getSource());
        e.setDistributor(input.getDistributor());
        e.setHasDOG(input.getHasDOG());
        return e;
    }

    private ImmutableSet<Location> transformLocations(org.atlasapi.media.entity.Encoding input) {
        return ImmutableSet.copyOf(Iterables.transform(input.getAvailableAt(),
            new Function<org.atlasapi.media.entity.Location, Location>() {
                @Override
                public Location apply(org.atlasapi.media.entity.Location input) {
                    return transformLocation(input);
                }
            }
        ));
    }
    
    private Location transformLocation(org.atlasapi.media.entity.Location input) {
        Location l = new Location();
        setIdentifiedFields(l, input);
        l.setAvailable(input.getAvailable());
        l.setTransportIsLive(input.getTransportIsLive());
        l.setTransportSubType(transformEnum(input.getTransportSubType(), TransportSubType.class));
        l.setTransportType(transformEnum(input.getTransportType(), TransportType.class));
        l.setUri(input.getUri());
        l.setEmbedCode(input.getEmbedCode());
        l.setEmbedId(input.getEmbedId());
        l.setPolicy(transformPolicy(input.getPolicy()));
        return l;
    }

    private Policy transformPolicy(org.atlasapi.media.entity.Policy input) {
        Policy p = new Policy();
        setIdentifiedFields(p, input);
        p.setAvailabilityStart(input.getAvailabilityStart());
        p.setAvailabilityEnd(input.getAvailabilityEnd());
        p.setDrmPlayableFrom(input.getDrmPlayableFrom());
        p.setAvailableCountries(input.getAvailableCountries());
        p.setAvailabilityLength(input.getAvailabilityLength());
        p.setRevenueContract(transformEnum(input.getRevenueContract(), Policy.RevenueContract.class));
        p.setPrice(input.getPrice());
        p.setPlatform(transformEnum(input.getPlatform(), Policy.Platform.class));
        p.setNetwork(transformEnum(input.getNetwork(), Policy.Network.class));
        p.setActualAvailabilityStart(input.getActualAvailabilityStart());
        return p;
    }

    private Broadcast transformBroadcast(org.atlasapi.media.entity.Broadcast input) {
        Broadcast b = new Broadcast(input.getBroadcastOn(), 
                input.getTransmissionTime(), input.getTransmissionEndTime());
        setIdentifiedFields(b, input);
        b.setScheduleDate(input.getScheduleDate());
        b.setIsActivelyPublished(input.isActivelyPublished());
        b.withId(input.getSourceId());
        b.setRepeat(input.getRepeat());
        b.setSubtitled(input.getSubtitled());
        b.setSigned(input.getSigned());
        b.setAudioDescribed(input.getAudioDescribed());
        b.setHighDefinition(input.getHighDefinition());
        b.setWidescreen(input.getWidescreen());
        b.setSurround(input.getSurround());
        b.setLive(input.getLive());
        b.setNewSeries(input.getNewSeries());
        b.setNewEpisode(input.getNewEpisode());
        b.setPremiere(input.getPremiere());
        return b;
    }
    
    private Restriction transformRestriction(org.atlasapi.media.entity.Restriction input) {
        Restriction r = new Restriction();
        setIdentifiedFields(r, input);
        r.setRestricted(input.isRestricted());
        r.setMinimumAge(input.getMinimumAge());
        r.setMessage(input.getMessage());
        return r;
    }

    private org.atlasapi.content.Content createSong(Song input) {
        org.atlasapi.content.Song s = new org.atlasapi.content.Song();
        s.setIsrc(input.getIsrc());
        s.setDuration(input.getDuration());
        return setItemFields(s, input);
    }

    private org.atlasapi.content.Content createFilm(Film input) {
        org.atlasapi.content.Film f = new org.atlasapi.content.Film();
        f.setWebsiteUrl(input.getWebsiteUrl());
        f.setSubtitles(Iterables.transform(input.getSubtitles(),
            new Function<Subtitles, org.atlasapi.content.Subtitles>() {
                @Override
                public org.atlasapi.content.Subtitles apply(Subtitles input) {
                    return new org.atlasapi.content.Subtitles(input.code());
                }
            }
        ));
        f.setReleaseDates(Iterables.transform(input.getReleaseDates(),
            new Function<ReleaseDate, org.atlasapi.content.ReleaseDate>() {
                @Override
                public org.atlasapi.content.ReleaseDate apply(ReleaseDate input) {
                    ReleaseType type = transformEnum(input.type(), ReleaseType.class);
                    return new org.atlasapi.content.ReleaseDate(input.date(), input.country(), type);
                }
            }
        ));
        return setItemFields(f, input);
    }

    private org.atlasapi.content.Content createEpisode(Episode input) {
        org.atlasapi.content.Episode e = new org.atlasapi.content.Episode();
        e.setEpisodeNumber(input.getEpisodeNumber());
        e.setPartNumber(e.getPartNumber());
        if (input.getSeriesRef() != null) {
            e.setSeriesRef(transformParentRef(input.getSeriesRef(), EntityType.SERIES));
        }
        e.setSpecial(input.getSpecial());
        return setItemFields(e, input);
    }

    private ParentRef transformParentRef(org.atlasapi.media.entity.ParentRef ref, EntityType type) {
        return new ParentRef(ref.getId(), type);
    }

    private <C extends org.atlasapi.content.Content> C setContentFields(C c) {
        return c;
    }

    @Override
    protected Iterable<Alias> moreAliases(Content input) {
        return ImmutableList.of();
    }
    
}
