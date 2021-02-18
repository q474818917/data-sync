package com.baletu.datasync.config.bind;


import com.baletu.datasync.config.common.PropertySource;

/**
 * The origin of a property, specifically its source and its name before any
 * prefix was removed.
 *
 * @author Andy Wilkinson
 * @since 1.3.0
 */
public class PropertyOrigin {

    private final PropertySource<?> source;

    private final String name;

    PropertyOrigin(PropertySource<?> source, String name){
        this.name = name;
        this.source = source;
    }

    public PropertySource<?> getSource() {
        return this.source;
    }

    public String getName() {
        return this.name;
    }
}
