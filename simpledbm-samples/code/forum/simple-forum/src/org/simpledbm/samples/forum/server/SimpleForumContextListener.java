package org.simpledbm.samples.forum.server;

import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class SimpleForumContextListener implements ServletContextListener {

    public void contextDestroyed(ServletContextEvent event) {
        ServletContext context = event.getServletContext();
        SimpleDBMContext sdbmContext = (SimpleDBMContext) context
                .getAttribute("org.simpledbm.samples.forum.sdbmContext");
        if (sdbmContext != null) {
            sdbmContext.destroy();
        }
        context.setAttribute("org.simpledbm.samples.forum.sdbmContext", null);
    }

    public void contextInitialized(ServletContextEvent event) {
        Properties properties = new Properties();
        SimpleDBMContext sdbmContext = new SimpleDBMContext(properties);
        ServletContext context = event.getServletContext();
        context.setAttribute("org.simpledbm.samples.forum.sdbmContext",
                sdbmContext);
    }

}
