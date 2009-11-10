package org.simpledbm.network.server;

import org.simpledbm.database.api.DatabaseFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SimpleDBMServer {

    private void usage() {
        System.out.println("SimpleDBMServer create|open <properties-file>");
    }
    
    public void run(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        Properties properties = parseProperties(args[1]);
        if (properties == null) {
            System.exit(1);
        }
        String command = args[0];
        if ("create".equalsIgnoreCase(command)) {
            create(properties);
        }
        else if ("open".equalsIgnoreCase(command)) {
            
        }
        else {
            usage();
            System.exit(1);
        }
    }

    private void create(Properties properties) {
        DatabaseFactory.create(properties);
    }

    private Properties parseProperties(String arg) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(arg);
        if (null == in) {
            System.out.println("Unable to access resource [" + arg + "]");
            return null;
        }
        Properties properties = new Properties();
        try {
            properties.load(in);
        }
        catch (IOException e) {
            System.err.println("Error loading from resource [" + arg + "] :" + e.getMessage());
            return null;
        }
        finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        System.out.println(properties);
        return properties;
    }

    public static void main(String[] args) {
        SimpleDBMServer server = new SimpleDBMServer();
        server.run(args);
    }
}
