package org.simpledbm.database;

import java.io.File;
import java.util.Properties;

import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

import junit.framework.TestCase;

public class DatabaseTests extends TestCase {

	public DatabaseTests() {
		super();
	}

	public DatabaseTests(String name) {
		super(name);
	}

    static Properties getServerProperties() {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "ctl.a");
        properties.setProperty("log.ctl.2", "ctl.b");
        properties.setProperty("log.groups.1.path", ".");
        properties.setProperty("log.archive.path", ".");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "16384");
        properties.setProperty("log.buffer.size", "16384");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "5");
        properties.setProperty("storage.basePath", "testdata/DatabaseTests");
        properties.setProperty("bufferpool.numbuffers", "50");
        properties.setProperty("logging.properties.type", "log4j");
        properties.setProperty("logging.propertis.file", "classpath:simpledbm.logging.properties");
        
        return properties;
    }

	
    public void testBasicFunctions() throws Exception {
    
    	deleteRecursively("testdata/DatabaseTests");
    	Database.create(getServerProperties());
    	
    	Database db = new Database(getServerProperties());
    	db.start();
    	try {
    		FieldFactory ff = db.getFieldFactory();
    		TypeDescriptor employee_rowtype[] = {
    			ff.getIntegerType(),	/* primary key */
    			ff.getVarcharType(20),	/* name */
    			ff.getVarcharType(20),  /* surname */
    			ff.getVarcharType(20),  /* city */
    			ff.getVarcharType(45),  /* email address */
    			ff.getDateTimeType(),	/* date of birth */
    			ff.getNumberType(2)		/* salary */
    		};
    		Table table = db.addTableDefinition("employee", 1, employee_rowtype);
    		table.addIndex(2, "employee1.idx", new int[] {0}, true, true);
    		table.addIndex(3, "employee2.idx", new int[] {2,1}, false, false);
    		table.addIndex(4, "employee3.idx", new int[] {5}, false, false);
    		table.addIndex(5, "employee4.idx", new int[] {6}, false, false);
    		
    		db.createTable(table);
    	}
    	finally {
    		db.shutdown();
    	}
    	
    	db = new Database(getServerProperties());
    	db.start();
    	try {
    		Table table = db.getTableDefinition(1);
    	}
    	finally {
    		db.shutdown();
    	}
    	
    	
    }
    
	void deleteRecursively(String pathname) {
		File file = new File(pathname);
		deleteRecursively(file);
	}

	void deleteRecursively(File dir) {
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					deleteRecursively(file);
				} else {
					file.delete();
				}
			}
		}
		dir.delete();
	}
}
