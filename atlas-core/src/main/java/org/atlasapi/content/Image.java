package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.DateTime;

import com.google.common.base.Predicate;
import com.metabroadcast.common.media.MimeType;

public class Image {
    
    public enum AspectRatio {
        SIXTEEN_BY_NINE("16x9"),
        FOUR_BY_THREE("4x3");
        
        private final String name;
        
        private AspectRatio(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
    }
    
    public enum Color {
        
        COLOR("color"),
        BLACK_AND_WHITE("black_and_white"),
        SINGLE_COLOR("single_color");
        
        private final String name;
        
        private Color(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
    }
    
    public enum Theme {
        
        DARK_OPAQUE("dark_opaque"), 
        LIGHT_OPAQUE("light_opaque"), 
        DARK_TRANSPARENT("dark_transparent"),
        LIGHT_TRANSPARENT("light_transparent");
        
        private final String name;
        
        private Theme(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
    }
    
    public enum Type {

        PRIMARY("primary"),
        ADDITIONAL("additional"),
        BOX_ART("box_art"),
        POSTER("poster"),
        LOGO("logo");
        
        private final String name;
        
        private Type(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
    }

    
    public static final Predicate<Image> IS_PRIMARY = new Predicate<Image>() {
        @Override
        public boolean apply(Image input) {
            return input.getType() != null && input.getType().equals(Type.PRIMARY);
        }
    };
    
    public static final Builder builder(Image base) {
        Builder builder = new Builder(base.getCanonicalUri());
        builder.withHeight(base.height);
        builder.withWidth(base.width);
        builder.withType(base.type);
        builder.withColor(base.color);
        builder.withTheme(base.theme);
        builder.withAspectRatio(base.aspectRatio);
        builder.withMimeType(base.mimeType);
        builder.withAvailabilityStart(base.availabilityStart);
        builder.withAvailabilityEnd(base.availabilityEnd);
        return builder;
    }
    
    public static final Builder builder(String uri) {
        return new Builder(uri);
    }
    
    public static final class Builder {

        private String uri;
        private Integer height;
        private Integer width;
        private Type type;
        private Color color;
        private Theme theme;
        private AspectRatio aspectRatio;
        private MimeType mimeType;
        private DateTime availabilityStart;
        private DateTime availabilityEnd;

        public Builder(String uri) {
            this.uri = uri;
        }
        
        public Builder withUri(String uri) {
            this.uri = uri;
            return this;
        }
        
        public Builder withHeight(Integer height) {
            this.height = height;
            return this;
        }
        
        public Builder withWidth(Integer width) {
            this.width = width;
            return this;
        }
        
        public Builder withType(Type type) {
            this.type = type;
            return this;
        }
        
        public Builder withColor(Color color) {
            this.color = color;
            return this;
        }
        
        public Builder withTheme(Theme theme) {
            this.theme = theme;
            return this;
        }
        
        public Builder withAspectRatio(AspectRatio aspectRatio) {
            this.aspectRatio = aspectRatio;
            return this;
        }
        
        public Builder withMimeType(MimeType mimeType) {
            this.mimeType = mimeType;
            return this;
        }
        
        public Builder withAvailabilityStart(DateTime availabilityStart) {
            this.availabilityStart = availabilityStart;
            return this;
        }
        
        public Builder withAvailabilityEnd(DateTime availabilityEnd) {
            this.availabilityEnd = availabilityEnd;
            return this;
        }

        public Image build() {
            Image image = new Image(uri);
            image.setHeight(height);
            image.setWidth(width);
            image.setType(type);
            image.setColor(color);
            image.setTheme(theme);
            image.setAspectRatio(aspectRatio);
            image.setMimeType(mimeType);
            image.setAvailabilityStart(availabilityStart);
            image.setAvailabilityEnd(availabilityEnd);
            return image;
        }
    }

    private String uri;
    private Type type;
    private Color color;
    private Theme theme;
    private Integer height;
    private Integer width;
    private AspectRatio aspectRatio;
    private MimeType mimeType;
    private DateTime availabilityStart;
    private DateTime availabilityEnd;
    
    public Image(String uri) {
        this.uri = checkNotNull(uri);
    }
    
    public String getCanonicalUri() {
        return uri;
    }
    
    public void setCanonicalUri(String uri) {
        this.uri = uri;
    }
    
    public Integer getHeight() {
        return height;
    }
    
    public void setHeight(Integer height) {
        this.height = height;
    }
    
    public Integer getWidth() {
        return width;
    }
    
    public void setWidth(Integer width) {
        this.width = width;
    }
    
    public Type getType() {
        return type;
    }
    
    public void setType(Type type) {
        this.type = type;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Theme getTheme() {
        return theme;
    }

    public void setTheme(Theme theme) {
        this.theme = theme;
    }

    public AspectRatio getAspectRatio() {
        return aspectRatio;
    }

    public void setAspectRatio(AspectRatio aspectRatio) {
        this.aspectRatio = aspectRatio;
    }

    public MimeType getMimeType() {
        return mimeType;
    }

    public void setMimeType(MimeType mimeType) {
        this.mimeType = mimeType;
    }
    
    public DateTime getAvailabilityStart() {
        return availabilityStart;
    }
    
    public void setAvailabilityStart(DateTime availabilityStart) {
        this.availabilityStart = availabilityStart;
    }
    
    public DateTime getAvailabilityEnd() {
        return availabilityEnd;
    }
    
    public void setAvailabilityEnd(DateTime availabilityEnd) {
        this.availabilityEnd = availabilityEnd;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Image) {
            Image other = (Image) that;
            return uri.equals(other.uri);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return uri.hashCode();
    }
    
    public static final Predicate<Image> IS_AVAILABLE = new Predicate<Image>() {
        @Override
        public boolean apply(Image input) {
            return (input.getAvailabilityStart() == null || ! (new DateTime(input.getAvailabilityStart()).isAfterNow()))
                    && (input.getAvailabilityEnd() == null || new DateTime(input.getAvailabilityEnd()).isAfterNow());
        }
    };

}
