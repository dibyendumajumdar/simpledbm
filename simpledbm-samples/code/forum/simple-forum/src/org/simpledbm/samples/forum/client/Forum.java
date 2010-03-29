package org.simpledbm.samples.forum.client;

import java.io.Serializable;

/**
 * A Forum contains topics. Forums must have a unique name.
 * 
 * @author dibyendumajumdar
 */
public class Forum implements Serializable {
    private static final long serialVersionUID = 1L;
    String name;
    String description;

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

}
