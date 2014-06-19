package org.talend.dataprofiler.benchmark.berkeleydb;

// file MyDbEnv.java

import java.io.File;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyDbEnv {

    private Environment dbEnv;

    // The databases that our application uses
    private Database dataDb;
    
    private final String CLASSCATALOGDBNAME="ClassCatalogDB";
    private final String DATADBNAME="dataDB";

    private Database classCatalogDb;

    // Needed for object serialization
    private StoredClassCatalog classCatalog;

    // Our constructor does nothing
    public BerkeleyDbEnv() {
    }

    // The setup() method opens all our databases and the environment
    // for us.
    public void setup(File envHome, boolean readOnly) throws DatabaseException {

        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        DatabaseConfig dbConfig = new DatabaseConfig();
        DatabaseConfig catalogConfig = new DatabaseConfig();

        // If the environment is read-only, then
        // make the databases read-only too.
        myEnvConfig.setReadOnly(readOnly);
        dbConfig.setReadOnly(readOnly);
        catalogConfig.setReadOnly(readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and databases if
        // they do not exist.
        myEnvConfig.setAllowCreate(!readOnly);
        dbConfig.setAllowCreate(!readOnly);
        catalogConfig.setAllowCreate(!readOnly);

        // Allow transactions if we are writing to the database
        myEnvConfig.setTransactional(!readOnly);
        dbConfig.setTransactional(!readOnly);
        catalogConfig.setTransactional(!readOnly);
        
        myEnvConfig.setCachePercent(10);
        myEnvConfig.setCacheSize(98304);

        // Open the environment
        dbEnv = new Environment(envHome, myEnvConfig);

        // Now open, or create and open, our databases
        // Open the vendors and inventory databases
        dataDb = dbEnv.openDatabase(null, DATADBNAME, dbConfig);

        // Open the class catalog db. This is used to
        // optimize class serialization.
        classCatalogDb = dbEnv.openDatabase(null, CLASSCATALOGDBNAME, dbConfig);

        // Create our class catalog
        classCatalog = new StoredClassCatalog(classCatalogDb);

    }

    // getter methods

    // Needed for things like beginning transactions
    public Environment getEnv() {
        return dbEnv;
    }

    public Database getDataDB() {
        return dataDb;
    }

    public StoredClassCatalog getClassCatalog() {
        return classCatalog;
    }

    /**
     * 
     * Clear database
     */
    public void clearDatabase() {
    	  dataDb.close();
          classCatalogDb.close();
        this.dbEnv.removeDatabase(null, CLASSCATALOGDBNAME);
        this.dbEnv.removeDatabase(null, DATADBNAME);
        
    }

    // Close the environment
    public void close() {
        if (dbEnv != null) {
            try {
                // Close the secondary before closing the primaries
                dataDb.close();
                classCatalogDb.close();
                this.dbEnv.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " + dbe.toString());
                System.exit(-1);
            }
        }
    }
}
