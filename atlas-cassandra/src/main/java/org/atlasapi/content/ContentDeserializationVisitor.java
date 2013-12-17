package org.atlasapi.content;

import static org.atlasapi.entity.ProtoBufUtils.deserializeDateTime;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Subtitle;
import org.atlasapi.serialization.protobuf.ContentProtos.Synopsis;
import org.atlasapi.source.Sources;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.intl.Countries;

public class ContentDeserializationVisitor implements ContentVisitor<Content> {

    private static final VersionsSerializer versionsSerializer = new VersionsSerializer();
    private static final TopicRefSerializer topicRefSerializer = new TopicRefSerializer();
    private static final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();
    private static final KeyPhraseSerializer keyPhraseSerializer = new KeyPhraseSerializer();
    private static final CrewMemberSerializer crewMemberSerializer = new CrewMemberSerializer();
    private static final ContainerSummarySerializer containerSummarySerializer = new ContainerSummarySerializer();
    private static final ReleaseDateSerializer releaseDateSerializer = new ReleaseDateSerializer();
    private static final CertificateSerializer certificateSerializer = new CertificateSerializer();
    
    private ContentProtos.Content msg;

    public ContentDeserializationVisitor(ContentProtos.Content msg) {
        this.msg = msg;
    }
    
    private <I extends Identified> I visitIdentified(I identified) {
        if (msg.hasId()) {
            identified.setId(Id.valueOf(msg.getId()));
        }
        if (msg.hasUri()) {
            identified.setCanonicalUri(msg.getUri());
        }
        if (msg.hasLastUpdated()) {
            identified.setLastUpdated(deserializeDateTime(msg.getLastUpdated()));
        }

        Builder<Alias> aliases = ImmutableSet.builder();
        for (CommonProtos.Alias alias : msg.getAliasesList()) {
            aliases.add(new Alias(alias.getNamespace(), alias.getValue()));
        }
        identified.setAliases(aliases.build());
        
        ImmutableSet.Builder<EquivalenceRef> equivRefs = ImmutableSet.builder();
        for (Reference equivRef : msg.getEquivsList()) {
            equivRefs.add(new EquivalenceRef(Id.valueOf(equivRef.getId()),
                Sources.fromPossibleKey(equivRef.getSource()).get()
            ));
        }
        identified.setEquivalentTo(equivRefs.build());
        return identified;
    }

    private <D extends Described> D visitDescribed(D described) {
        described = visitIdentified(described);
        described.setPublisher(Sources.fromPossibleKey(msg.getSource()).get());
        if (msg.hasFirstSeen()) {
            described.setFirstSeen(ProtoBufUtils.deserializeDateTime(msg.getFirstSeen()));
        }
        if (msg.hasChildLastUpdated()) {
            described.setThisOrChildLastUpdated(ProtoBufUtils.deserializeDateTime(msg.getChildLastUpdated()));
        }
        if (msg.hasMediaType()) {
            described.setMediaType(MediaType.fromKey(msg.getMediaType()).get());
        }
        if (msg.getTitlesCount() > 0) {
            described.setTitle(msg.getTitles(0).getValue());
        }
        if (msg.hasDescription()) {
            described.setDescription(msg.getDescription());
        }
        if (msg.hasImage()) {
            described.setImage(msg.getImage());
        }
        if (msg.hasThumb()) {
            described.setThumbnail(msg.getThumb());
        }
        if (msg.getSynopsesCount() > 0) {
            Synopsis synopsis = msg.getSynopses(0);
            if (synopsis.hasShort()) {
                described.setShortDescription(synopsis.getShort());
            }
            if (synopsis.hasMedium()) {
                described.setMediumDescription(synopsis.getMedium());
            }
            if (synopsis.hasLong()) {
                described.setLongDescription(synopsis.getLong());
            }
        }
        
        ImmutableSet.Builder<Image> images = ImmutableSet.builder();
        for (ContentProtos.Image image : msg.getImagesList()) {
            images.add(new Image(image.getUri()));
        }
        described.setImages(images.build());
        described.setGenres(msg.getGenresList());
        described.setPresentationChannel(msg.getPresentationChannel());
        if (msg.hasScheduleOnly()) {
            described.setScheduleOnly(msg.getScheduleOnly());
        }
        if (msg.hasSpecialization()) {
            described.setSpecialization(Specialization.valueOf(msg.getSpecialization().toUpperCase()));
        }
        return described;
    }
    
    private <C extends Content> C visitContent(C content) {
        content = visitDescribed(content);
        ImmutableSet.Builder<Certificate> certificates = ImmutableSet.builder();
        for (ContentProtos.Certificate cert : msg.getCertificatesList()) {
            certificates.add(certificateSerializer.deserialize(cert));
        }
        content.setCertificates(certificates.build());

        ImmutableSet.Builder<CrewMember> crew = ImmutableSet.builder();
        for (ContentProtos.CrewMember crewMember : msg.getCrewMembersList()) {
            crew.add(crewMemberSerializer.deserialize(crewMember));
        }
        content.setPeople(crew.build().asList());
        
        ImmutableSet.Builder<Clip> clips = ImmutableSet.builder();
        for (ContentProtos.Content clipPb : msg.getClipsList()) {
            clips.add((Clip) new Clip().accept(new ContentDeserializationVisitor(clipPb)));
        }
        content.setClips(clips.build());
        
        ImmutableSet.Builder<ContentGroupRef> groupRefs = ImmutableSet.builder();
        for (Reference groupRef : msg.getContentGroupsList()) {
            groupRefs.add(new ContentGroupRef(
                Id.valueOf(groupRef.getId()),""
            ));
        }
        content.setContentGroupRefs(groupRefs.build());
        
        ImmutableSet.Builder<KeyPhrase> phrases = ImmutableSet.builder();
        for (ContentProtos.KeyPhrase phrase : msg.getKeyPhrasesList()) {
            phrases.add(keyPhraseSerializer.deserialize(phrase));
        }
        content.setKeyPhrases(phrases.build());
        
        content.setLanguages(msg.getLanguageList());
        
        ImmutableSet.Builder<RelatedLink> links = ImmutableSet.builder();
        for (int i = 0; i < msg.getRelatedLinkCount(); i++) {
            links.add(relatedLinkSerializer.deserialize(msg.getRelatedLink(i)));
        }
        content.setRelatedLinks(links.build());
        
        ImmutableSet.Builder<TopicRef> topicRefs = ImmutableSet.builder();
        for (int i = 0; i < msg.getTopicRefsCount(); i++) {
            topicRefs.add(topicRefSerializer.deserialize(msg.getTopicRefs(i)));
        }
        content.setTopicRefs(topicRefs.build());
        
        if (msg.hasYear()) {
            content.setYear(msg.getYear());
        }
        
        return content;
    }

    private <C extends Container> C visitContainer(C container) {
        container = visitContent(container);
        ChildRefSerializer childRefSerializer = new ChildRefSerializer(container.getPublisher());
        ImmutableSet.Builder<ItemRef> childRefs = ImmutableSet.builder();
        for (int i = 0; i < msg.getChildrenCount(); i++) {
            childRefs.add(childRefSerializer.deserialize(msg.getChildren(i)));
        }
        container.setItemRefs(Ordering.natural().immutableSortedCopy(childRefs.build()));
        return container;
    }

    @Override
    public Brand visit(Brand brand) {
        brand = visitContainer(brand);
        ImmutableSet.Builder<SeriesRef> childRefs = ImmutableSet.builder();
        SeriesRefSerializer seriesRefSerializer = new SeriesRefSerializer(brand.getPublisher());
        for (int i = 0; i < msg.getSecondariesCount(); i++) {
            childRefs.add(seriesRefSerializer.deserialize(msg.getSecondaries(i)));
        }
        brand.setSeriesRefs(SeriesRef.dedupeAndSort(childRefs.build()));
        return brand;
    }

    @Override
    public Series visit(Series series) {
        series = visitContainer(series);
        if (msg.hasContainerRef()) {
            series.setBrandRef(new BrandRef(
                Id.valueOf(msg.getContainerRef().getId()),
                Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(series.getPublisher())
            ));
        }
        series.withSeriesNumber(msg.hasSeriesNumber() ? msg.getSeriesNumber() : null);
        series.setTotalEpisodes(msg.hasTotalEpisodes() ? msg.getTotalEpisodes() : null);
        return series;
    }

    @Override
    public Episode visit(Episode episode) {
        episode = visitItem(episode);
        if (msg.hasSeriesRef()) {
            episode.setSeriesRef(new SeriesRef(
                Id.valueOf(msg.getSeriesRef().getId()),
                Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(episode.getPublisher())
            ));
        }
        episode.setSeriesNumber(msg.hasSeriesNumber() ? msg.getSeriesNumber() : null);
        episode.setEpisodeNumber(msg.hasEpisodeNumber() ? msg.getEpisodeNumber() : null);
        episode.setPartNumber(msg.hasPartNumber() ? msg.getPartNumber() : null);
        return episode;
    }

    @Override
    public Film visit(Film film) {
        film = visitItem(film);
        ImmutableSet.Builder<ReleaseDate> releaseDates = ImmutableSet.builder();
        for (int i = 0; i < msg.getReleaseDatesCount(); i++) {
            releaseDates.add(releaseDateSerializer.deserialize(msg.getReleaseDates(i)));
        }
        film.setReleaseDates(releaseDates.build());
        film.setWebsiteUrl(msg.hasWebsiteUrl() ? msg.getWebsiteUrl() : null);
        ImmutableSet.Builder<Subtitles> subtitles = ImmutableSet.builder();
        for (Subtitle sub : msg.getSubtitlesList()) {
            subtitles.add(new Subtitles(sub.getLanguage()));
        }
        film.setSubtitles(subtitles.build());
        return film;
    }

    @Override
    public Song visit(Song song) {
        song = visitItem(song);
        song.setIsrc(msg.hasIsrc() ? msg.getIsrc() : null);
        song.setDuration(msg.hasDuration() ? Duration.millis(msg.getDuration()) : null);
        return song;
    }

    @Override
    public Item visit(Item item) {
        return visitItem(item);
    }

    private <I extends Item> I visitItem(I item) {
        item = visitContent(item);
        if (msg.hasContainerRef()) {
            item.setContainerRef(toContainerRef(msg.getContainerRef(), item.getPublisher()));
        }
        if (msg.hasContainerSummary()) {
            item.setContainerSummary(containerSummarySerializer.deserialize(msg.getContainerSummary()));
        }
        if (msg.hasBlackAndWhite()) {
            item.setBlackAndWhite(msg.getBlackAndWhite());
        }
        item.setCountriesOfOrigin(Countries.fromCodes(msg.getCountriesList()));
        if (msg.hasLongform()) {
            item.setIsLongForm(msg.getLongform());
        }
        item.setVersions(versionsSerializer.deserialize(msg));
        return item;
    }
    
    private ContainerRef toContainerRef(Reference containerRef, final Publisher publisher) {
        ContentType type = ContentType.fromKey(containerRef.getType()).get();
        
        return type.accept(new ContentType.Visitor<ContainerRef>() {
            
            @Override
            public ContainerRef visitBrand(ContentType contentType) {
                return new BrandRef(
                    Id.valueOf(msg.getContainerRef().getId()),
                    Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(publisher)
                );
            }
            
            @Override
            public ContainerRef visitSeries(ContentType contentType) {
                return new SeriesRef(
                    Id.valueOf(msg.getContainerRef().getId()),
                    Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(publisher)
                );
            }

            @Override
            public ContainerRef visitClip(ContentType type) {
                throw new IllegalArgumentException("Can't create ContainerRef for type " + type);
            }

            @Override
            public ContainerRef visitSong(ContentType type) {
                throw new IllegalArgumentException("Can't create ContainerRef for type " + type);
            }

            @Override
            public ContainerRef visitFilm(ContentType type) {
                throw new IllegalArgumentException("Can't create ContainerRef for type " + type);
            }

            @Override
            public ContainerRef visitEpisode(ContentType type) {
                throw new IllegalArgumentException("Can't create ContainerRef for type " + type);
            }

            @Override
            public ContainerRef visitItem(ContentType type) {
                throw new IllegalArgumentException("Can't create ContainerRef for type " + type);
            }
            
        });
    }

    @Override
    public Content visit(Clip clip) {
        return visitItem(clip);
    }
    
    
}