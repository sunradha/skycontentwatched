package com.sky.playevents.consumerstream;
/**
 * This Class establishes connection with MySql database (PlayEventDB database) and creates a Connection pool of 5
 * connections. The getConnectionFromPool method then returns the Connection object for SQL transactions.
 * @author Sunil Sarda
 * @version 1.0
 * @since   2019-09-13
 */
import com.sky.playevents.configuration.ReadPropertyFile;
import com.sky.playevents.producers.PlayEventProducer;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionPool {

    static Logger logger = Logger.getLogger(ConnectionPool.class.getName());
    private static ConnectionPool connectionPoolInstance = null;
    private static GenericObjectPool gPool = null;

    /**
     * This method returns the Singleton ConnectionPool object
     * @return Returns Connection Pool object
     */
    public static ConnectionPool getInstance() {
        if (connectionPoolInstance == null) {
            connectionPoolInstance = new ConnectionPool();
        }
        return connectionPoolInstance;
    }

    /**
     * This method Sets up the Connection pool and returns DataSource Object to the calling method
     * getConnectionFromPool. The Connection pool size can be set in the config.properties file.
     * @return Returns DataSource object
     */
    public DataSource setUpPool() {

        Properties configProperties = ReadPropertyFile.loadProperties();

        final String JDBC_DB_URL = configProperties.getProperty("jdbcURL");
        final String JDBC_USER = configProperties.getProperty("mySqlUser");
        final String JDBC_PASS = configProperties.getProperty("mySqlPwd");

        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
        gPool = new GenericObjectPool();
        gPool.setMaxActive(Integer.parseInt(configProperties.getProperty("connectionPoolSize")));

        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);

        // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory
        // to Add Object Pooling Functionality!
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null,
                false, true);
        return new PoolingDataSource(gPool);
    }

    /**
     * This method returns the Connection object to the requesting methods performing SQL transactions.
     * @return Returns Connection object.
     */
    public static Connection getConnectionFromPool(){

        Connection connObj;
        ConnectionPool jdbcObj = new ConnectionPool();

        try {
            DataSource dataSource = jdbcObj.setUpPool();
            connObj = dataSource.getConnection();
            connObj.setAutoCommit(false);
            return connObj;

        } catch (SQLException e) {
            logger.log(Level.SEVERE, "SQLException while creating Connection Object : " + e.toString());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while creating Connection Object : " + e.toString());
        }
        return null;
    }
}