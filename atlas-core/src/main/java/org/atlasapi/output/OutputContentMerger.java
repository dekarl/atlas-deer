package org.atlasapi.output;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.ChildRef;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.Described;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.Person;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.TopicRef;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.equiv.SeriesAndEpisodeNumber;
import org.atlasapi.equiv.SeriesOrder;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;


public class OutputContentMerger implements EquivalentsMergeStrategy<Content> {
    
    private static final Ordering<Episode> SERIES_ORDER = Ordering.from(new SeriesOrder());

    @SuppressWarnings("unchecked")
    @Deprecated
    public <T extends Described> List<T> merge(ApplicationSources sources, List<T> contents) {
        Ordering<Sourced> publisherComparator = sources.getSourcedReadOrdering();
        List<T> merged = Lists.newArrayListWithCapacity(contents.size());
        Set<T> processed = Sets.newHashSet();

        for (T content : contents) {
            if (processed.contains(content)) {
                continue;
            }
            List<T> same = publisherComparator.sortedCopy(findSame(content, contents));
            processed.addAll(same);

            T chosen = same.get(0);
            
            chosen.setId(lowestId(chosen));

            // defend against broken transitive equivalence
            if (merged.contains(chosen)) {
                continue;
            }

            List<T> notChosen = same.subList(1, same.size());

            if (chosen instanceof Container) {
                mergeIn(sources, (Container) chosen, (List<Container>) notChosen);
            }
            if (chosen instanceof Item) {
                mergeIn(sources, (Item) chosen, (List<Item>) notChosen);
            }
            if (chosen instanceof ContentGroup) {
                mergeIn(sources, (ContentGroup) chosen, (List<ContentGroup>) notChosen);
            }
            merged.add(chosen);
        }
        return merged;
    }
    
    private <T extends Described> Id lowestId(T chosen) {
        Id lowest = chosen.getId();
        for (EquivalenceRef ref : chosen.getEquivalentTo()) {
            Id candidate = ref.getId();
            lowest = Ordering.natural().nullsLast().min(lowest, candidate);
        }
        return lowest;
    }
        
    @Override
    @SuppressWarnings("unchecked")
    public <T extends Content> T merge(T chosen, final Iterable<T> equivalents, final ApplicationSources sources) {
        chosen.setId(lowestId(chosen));
        return chosen.accept(new ContentVisitorAdapter<T>() {
            
            @Override
            protected T visitContainer(Container container) {
                mergeIn(sources, container, (List<Container>) equivalents);
                return (T) container;
            }
            
            @Override
            protected T visitItem(Item item) {
                mergeIn(sources, item, (List<Item>) equivalents);
                return (T) item;
            }
            
        });
    }
    
    @SuppressWarnings("unchecked")
    private <T extends Described> List<T> findSame(T brand, Iterable<T> contents) {
        List<T> same = Lists.newArrayList(brand);
        for (T possiblyEquivalent : contents) {
            if (!brand.equals(possiblyEquivalent) && possiblyEquivalent.isEquivalentTo(brand)) {
                same.add(possiblyEquivalent);
            }
        }
        return same;
    }

    private <T extends ContentGroup> void mergeIn(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        mergeDescribed(sources, chosen, notChosen);
        for (ContentGroup contentGroup : notChosen) {
            for (ChildRef childRef : contentGroup.getContents()) {
                chosen.addContent(childRef);
            }
        }
        if (chosen instanceof Person) {
            Person person = (Person) chosen;
            ImmutableSet.Builder<String> quotes = ImmutableSet.builder();
            quotes.addAll(person.getQuotes());
            for (Person unchosen : Iterables.filter(notChosen, Person.class)) {
                quotes.addAll(unchosen.getQuotes());
                person.withName(person.name() != null ? person.name() : unchosen.name());
                person.setGivenName(person.getGivenName() != null ? person.getGivenName() : unchosen.getGivenName());
                person.setFamilyName(person.getFamilyName() != null ? person.getFamilyName() : unchosen.getFamilyName());
                person.setGender(person.getGender() != null ? person.getGender() : unchosen.getGender());
                person.setBirthDate(person.getBirthDate() != null ? person.getBirthDate() : unchosen.getBirthDate());
                person.setBirthPlace(person.getBirthPlace() != null ? person.getBirthPlace() : unchosen.getBirthPlace());
            }
            person.setQuotes(quotes.build());
        }
    }
    
    private <T extends Described> void mergeDescribed(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        applyImagePrefs(sources, chosen, notChosen);
        chosen.setRelatedLinks(projectFieldFromEquivalents(chosen, notChosen, new Function<T, Iterable<RelatedLink>>() {
            @Override
            public Iterable<RelatedLink> apply(T input) {
                return input.getRelatedLinks();
            }
        }));
        if (chosen.getTitle() == null) {
            chosen.setTitle(first(notChosen, TO_TITLE));
        }
        if (chosen.getDescription() == null) {
            chosen.setDescription(first(notChosen, TO_DESCRIPTION));
        }
        if (chosen.getLongDescription() == null) {
            chosen.setLongDescription(first(notChosen, TO_LONG_DESCRIPTION));
        }
        if (chosen.getMediumDescription() == null) {
            chosen.setMediumDescription(first(notChosen, TO_MEDIUM_DESCRIPTION));
        }
        if (chosen.getShortDescription() == null) {
            chosen.setShortDescription(first(notChosen, TO_SHORT_DESCRIPTION));
        }
    }

    private <T extends Described, P> Iterable<P> projectFieldFromEquivalents(T chosen,
            Iterable<T> notChosen, Function<T, Iterable<P>> projector) {
        return Iterables.concat(
                projector.apply(chosen), 
                Iterables.concat(Iterables.transform(notChosen, projector))
            );
    }
    
    private <I extends Described, O> O first(Iterable<I> is, Function<? super I, ? extends O> transform) {
        return Iterables.getFirst(Iterables.filter(Iterables.transform(is, transform), Predicates.notNull()), null);
    }
    
    private <T extends Content> void mergeContent(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        mergeDescribed(sources, chosen, notChosen);
        for (T notChosenItem : notChosen) {
            for (Clip clip : notChosenItem.getClips()) {
                chosen.addClip(clip);
            }
        }
        mergeTopics(chosen, notChosen);
        mergeKeyPhrases(chosen, notChosen);
    }

    private <T extends Item> void mergeIn(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        mergeContent(sources, chosen, notChosen);
        mergeVersions(sources, chosen, notChosen);
        if (chosen instanceof Film) {
            mergeFilmProperties(sources, (Film) chosen, Iterables.filter(notChosen, Film.class));
        }
    }

    private <T extends Content> void mergeKeyPhrases(T chosen, Iterable<T> notChosen) {
        chosen.setKeyPhrases(projectFieldFromEquivalents(chosen, notChosen, new Function<T, Iterable<KeyPhrase>>() {
            @Override
            public Set<KeyPhrase> apply(T input) {
                return input.getKeyPhrases();
            }
        }));
    }

    private <T extends Content> void mergeTopics(T chosen, Iterable<T> notChosen) {
        Function<T, Iterable<TopicRef>> topicRefsProjector = new Function<T, Iterable<TopicRef>>() {
            @Override
            public Iterable<TopicRef> apply(T input) {
                return Iterables.transform(input.getTopicRefs(), new TopicPublisherSetter(input));
            }
        };
        chosen.setTopicRefs(projectFieldFromEquivalents(chosen, notChosen, topicRefsProjector));
    }

    private void mergeFilmProperties(ApplicationSources sources, Film chosen, Iterable<Film> notChosen) {
        Builder<Subtitles> subtitles = ImmutableSet.<Subtitles>builder().addAll(chosen.getSubtitles());
        Builder<String> languages = ImmutableSet.<String>builder().addAll(chosen.getLanguages());
        Builder<Certificate> certs = ImmutableSet.<Certificate>builder().addAll(chosen.getCertificates());
        Builder<ReleaseDate> releases = ImmutableSet.<ReleaseDate>builder().addAll(chosen.getReleaseDates());

        for (Film film : notChosen) {
            subtitles.addAll(film.getSubtitles());
            languages.addAll(film.getLanguages());
            certs.addAll(film.getCertificates());
            releases.addAll(film.getReleaseDates());
        }

        chosen.setSubtitles(subtitles.build());
        chosen.setLanguages(languages.build());
        chosen.setCertificates(certs.build());
        chosen.setReleaseDates(releases.build());

        if (sources.peoplePrecedenceEnabled()) {
            Iterable<Film> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
            List<Film> topFilmMatches = sources.getSourcedPeoplePrecedenceOrdering().leastOf(Iterables.filter(all, HAS_PEOPLE), 1);
            if (!topFilmMatches.isEmpty()) {
                Film top = topFilmMatches.get(0);
                chosen.setPeople(top.getPeople());
            }
        }
    }

    private <T extends Described> void applyImagePrefs(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        if (sources.imagePrecedenceEnabled()) {
            Iterable<T> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
            List<T> topImageMatches = sources.getSourcedImagePrecedenceOrdering().leastOf(Iterables.filter(all, HAS_AVAILABLE_IMAGE_SET), 1);
            if (!topImageMatches.isEmpty()) {
                T top = topImageMatches.get(0);
                chosen.setImage(top.getImage());
                chosen.setThumbnail(top.getThumbnail());
                chosen.setImages(top.getImages());
            } else {
                chosen.setImage(null);
            }
        }
    }

    private <T extends Item> void mergeVersions(ApplicationSources sources, T chosen, Iterable<T> notChosen) {
        // if chosen has broadcasts, merge the set of broadcasts from notChosen
        Set<Broadcast> chosenBroadcasts = Sets.newHashSet(Iterables.concat(Iterables.transform(chosen.getVersions(), Version.TO_BROADCASTS)));
        if (!chosenBroadcasts.isEmpty()) {
            List<T> notChosenOrdered = sources.getSourcedReadOrdering().sortedCopy(notChosen);
            for (Broadcast chosenBroadcast : chosenBroadcasts) {
                matchAndMerge(chosenBroadcast, notChosenOrdered);
            }
        }
        for (T notChosenItem : notChosen) {
            for (Version version : notChosenItem.getVersions()) {
                // TODO When we have more granular precedence this broadcast masking can be removed
                version.setBroadcasts(Sets.<Broadcast>newHashSet());
                chosen.addVersion(version);
            }
        }
    }
    
    private <T extends Item> void matchAndMerge(final Broadcast chosenBroadcast, List<T> notChosen) {
        List<Broadcast> equivBroadcasts = Lists.newArrayList();
        for (T notChosenItem : notChosen) {
            Iterable<Broadcast> notChosenBroadcasts = Iterables.concat(Iterables.transform(notChosenItem.getVersions(), Version.TO_BROADCASTS));
            Optional<Broadcast> matched = Iterables.tryFind(notChosenBroadcasts, new Predicate<Broadcast>() {
                @Override
                public boolean apply(Broadcast input) {
                    return chosenBroadcast.getBroadcastOn().equals(input.getBroadcastOn())
                            && chosenBroadcast.getTransmissionTime().equals(input.getTransmissionTime());
                }
             });
            if (matched.isPresent()) {
                equivBroadcasts.add(matched.get());
            }
        }
        // equivB'casts = list of matched broadcasts, ordered by precedence
        for (Broadcast equiv : equivBroadcasts) {
            mergeBroadcast(chosenBroadcast, equiv);
        }
    }
    
    private void mergeBroadcast(Broadcast chosen, Broadcast toMerge) {
        chosen.addAliases(toMerge.getAliases());
        chosen.addAliasUrls(toMerge.getAliasUrls());
        
        if (chosen.getRepeat() == null && toMerge.getRepeat() != null) {
            chosen.setRepeat(toMerge.getRepeat());
        }
        if (chosen.getScheduleDate() == null && toMerge.getScheduleDate() != null) {
            chosen.setScheduleDate(toMerge.getScheduleDate());
        }
        if (chosen.getSourceId() == null && toMerge.getSourceId() != null) {
            chosen.withId(toMerge.getSourceId());
        }
        if (chosen.getSubtitled() == null && toMerge.getSubtitled() != null) {
            chosen.setSubtitled(toMerge.getSubtitled());
        }
        if (chosen.getSigned() == null && toMerge.getSigned() != null) {
            chosen.setSigned(toMerge.getSigned());
        }
        if (chosen.getAudioDescribed() == null && toMerge.getAudioDescribed() != null) {
            chosen.setAudioDescribed(toMerge.getAudioDescribed());
        }
        if (chosen.getHighDefinition() == null && toMerge.getHighDefinition() != null) {
            chosen.setHighDefinition(toMerge.getHighDefinition());
        }
        if (chosen.getWidescreen() == null && toMerge.getWidescreen() != null) {
            chosen.setWidescreen(toMerge.getWidescreen());
        }
        if (chosen.getSurround() == null && toMerge.getSurround() != null) {
            chosen.setSurround(toMerge.getSurround());
        }
        if (chosen.getLive() == null && toMerge.getLive() != null) {
            chosen.setLive(toMerge.getLive());
        }
        if (chosen.getNewSeries() == null && toMerge.getNewSeries() != null) {
            chosen.setNewSeries(toMerge.getNewSeries());
        }
        if (chosen.getPremiere() == null && toMerge.getPremiere() != null) {
            chosen.setPremiere(toMerge.getPremiere());
        }
    }
    
    private static final Predicate<Described> HAS_AVAILABLE_IMAGE_SET = new Predicate<Described>() {

        @Override
        public boolean apply(Described content) {
            if (content.getImage() == null) {
                return false;
            }
            return isImageAvailable(content.getImage(), content.getImages());
        }
    };
    
    private static boolean isImageAvailable(String imageUri, Iterable<Image> images) {
        
        // Fneh. Image URIs differ between the image attribute and the canonical URI on Images.
        // See PaProgrammeProcessor for why.
        String rewrittenUri = imageUri.replace("http://images.atlasapi.org/pa/",
                "http://images.atlas.metabroadcast.com/pressassociation.com/");
        
        // If there is a corresponding Image object for this URI, we check its availability
        for (Image image : images) {
            if (image.getCanonicalUri().equals(rewrittenUri)) {
               return Image.IS_AVAILABLE.apply(image); 
            }
        }
        // Otherwise, we can only assume the image is available as we know no better
        return true;
    }
    
    private static final Predicate<Film> HAS_PEOPLE = new Predicate<Film>() {

        @Override
        public boolean apply(Film film) {
            return film.getPeople() != null && !film.getPeople().isEmpty();
        }
    };
    private static final Function<Described,String> TO_TITLE = new Function<Described,String>(){
        @Override
        @Nullable
        public String apply(@Nullable Described input) {
            return input == null ? null : input.getTitle();
        }};
    private static final Function<Described, String> TO_DESCRIPTION = new Function<Described, String>() {
        @Override
        public String apply(@Nullable Described input) {
            return input == null ? null : input.getDescription();
        }
    };
    private static final Function<Described, String> TO_LONG_DESCRIPTION = new Function<Described, String>() {
        @Override
        public String apply(@Nullable Described input) {
            return input == null ? null : input.getLongDescription();
        }
    };
    private static final Function<Described, String> TO_MEDIUM_DESCRIPTION = new Function<Described, String>() {
        @Override
        public String apply(@Nullable Described input) {
            return input == null ? null : input.getMediumDescription();
        }
    };
    private static final Function<Described, String> TO_SHORT_DESCRIPTION = new Function<Described, String>() {
        @Override
        public String apply(@Nullable Described input) {
            return input == null ? null : input.getShortDescription();
        }
    };

    public <T extends Item> void mergeIn(ApplicationSources sources, Container chosen, List<Container> notChosen) {
        mergeContent(sources, chosen, notChosen);
    }

    enum ItemIdStrategy {

        SERIES_EPISODE_NUMBER {

            @Override
            public Predicate<Item> match() {
                return new Predicate<Item>() {

                    @Override
                    public boolean apply(Item item) {
                        if (item instanceof Episode) {
                            Episode episode = (Episode) item;
                            return episode.getSeriesNumber() != null && episode.getEpisodeNumber() != null;
                        }
                        return false;
                    }
                };
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T extends Item> Iterable<T> merge(List<T> items, List<T> matches) {
                Map<SeriesAndEpisodeNumber, Episode> chosenItemLookup = Maps.newHashMap();
                for (T item : Iterables.concat(items, matches)) {
                    Episode episode = (Episode) item;
                    SeriesAndEpisodeNumber se = new SeriesAndEpisodeNumber(episode);
                    if (!chosenItemLookup.containsKey(se)) {
                        chosenItemLookup.put(se, episode);
                    } else {
                        Item chosen = chosenItemLookup.get(se);
                        for (Clip clip : item.getClips()) {
                            chosen.addClip(clip);
                        }
                    }
                }

                return (Iterable<T>) SERIES_ORDER.immutableSortedCopy(chosenItemLookup.values());
            }
        };

        protected abstract Predicate<Item> match();

        static ItemIdStrategy findBest(Iterable<? extends Item> items) {
            if (Iterables.all(items, ItemIdStrategy.SERIES_EPISODE_NUMBER.match())) {
                return SERIES_EPISODE_NUMBER;
            }
            return null;
        }

        public abstract <T extends Item> Iterable<T> merge(List<T> items, List<T> matches);
    }

    private final static class TopicPublisherSetter implements Function<TopicRef, TopicRef> {

        private final Content publishedContent;

        public TopicPublisherSetter(Content publishedContent) {
            this.publishedContent = publishedContent;
        }

        @Override
        public TopicRef apply(TopicRef input) {
            input.setPublisher(publishedContent.getPublisher());
            return input;
        }
    }
}
