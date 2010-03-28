package org.simpledbm.samples.forum.client;

public class Forum {
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
