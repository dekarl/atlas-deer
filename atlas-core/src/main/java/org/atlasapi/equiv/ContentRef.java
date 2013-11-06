package org.atlasapi.equiv;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.content.Described;
import org.atlasapi.content.Item;
import org.atlasapi.content.ParentRef;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Objects;

public class ContentRef implements Identifiable, Sourced {

    public static ContentRef valueOf(Described content) {
        Publisher publisher = content.getPublisher();
        Id id = content.getId();
        Id parentId = null;
        if (content instanceof Item) {
            parentId = uriOrNull(((Item)content).getContainer());
        }
        if (content instanceof Series) {
            parentId = uriOrNull(((Series)content).getParent());
        }
        return new ContentRef(id, publisher, parentId);
    }
    
    public static final Function<Described, ContentRef> FROM_CONTENT = new Function<Described,ContentRef>(){
        @Override
        public ContentRef apply(@Nullable Described input) {
            return ContentRef.valueOf(input);
        };
    };

    protected static Id uriOrNull(ParentRef parent) {
        return parent == null ? null : parent.getId();
    }
    
    private final Id id;
    private final Publisher publisher;
    private final Id parentId;
    
    public ContentRef(long id, Publisher publisher, @Nullable Id parentId) {
        this(Id.valueOf(id), publisher, parentId);
    }

    public ContentRef(Id id, Publisher publisher, @Nullable Id parentId) {
        this.id = checkNotNull(id);
        this.publisher = checkNotNull(publisher);
        this.parentId = parentId;
    }
    
    public Id getId() {
        return this.id;
    }
    
    public Publisher getPublisher() {
        return this.publisher;
    }
    
    public Id getParentId() {
        return this.parentId;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ContentRef) {
            ContentRef other = (ContentRef) that; 
            return id.equals(other.id) 
                && Objects.equal(parentId, other.parentId);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("id", id)
                .add("publisher", publisher)
                .add("parent", parentId)
                .toString();
    }
}
