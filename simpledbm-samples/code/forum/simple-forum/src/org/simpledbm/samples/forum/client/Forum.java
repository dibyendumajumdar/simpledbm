package org.simpledbm.samples.forum.client;

import java.io.Serializable;

/**
 * A Forum contains topics. Forums must have a unique name.
 * 
 * @author dibyendumajumdar
 */
@SuppressWarnings("serial")
public class Forum implements Serializable {
    String name;
    String description;

    public Forum() {
    }

    public Forum(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "Forum [description=" + description + ", name=" + name + "]";
    }

}
