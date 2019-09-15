package com.sky.playevents.configuration;

/**
 * This Singleton class Reads the config properties file and sets the properties for both Kafka Producers
 * and the Spark Kafka Direct Stream. The config file needs to be present in /src/main/resources folder
 *
 *
 * @author Sunil Sarda
 * @version 1.0
 * @since   2019-09-13
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadPropertyFile {

    static Logger logger = Logger.getLogger(ReadPropertyFile.class.getName());
    private static ReadPropertyFile propertiesSingleInstance = null;

    /**
     * Private Constructor to Enforce Singleton behaviour
     */
    private ReadPropertyFile() {
    }

    /**
     * This method checks if there's any already created instance of ReadPropertyFile class.
     * If not it creates one and returns.
     * @return Returns ReadPropertyFile Singleton Object
     */
    public static ReadPropertyFile getInstance() {
        if (propertiesSingleInstance == null) {
            propertiesSingleInstance = new ReadPropertyFile();
        }
        return propertiesSingleInstance;
    }

    /**
     * This method loads the properties file config.properties present in /src/main/resources folder
     * into Properties Object and returns it.
     * @return Returns Properties Object after loading all the properties from config.properties.
     */
    public static Properties loadProperties() {

        try (InputStream input = ReadPropertyFile.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                logger.log(Level.SEVERE, "Sorry, unable to find config.properties file ");
                return null;
            } else {
                prop.load(input);
                return prop;
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IO Exception in Config file : " + e.toString());
        }
        return null;
    }

    /**
     * This Method takes the already loaded Properties Object (All properties from config file) and
     * creates another Properties Object to load Kafka Producer Specific properties and returns
     * @param properties Object with all the properties from config file.
     * @return Returns Properties Object with only Kafka Producer specific properties.
     */
    public static Properties getKafkaProducerProperties(Properties properties) {

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", properties.getProperty("bootstrap.servers"));
        props.setProperty("key.serializer", properties.getProperty("key.serializer"));
        props.setProperty("value.serializer", properties.getProperty("value.serializer"));
        props.setProperty("schema.registry.url", properties.getProperty("schema.registry.url"));

        return props;
    }

    /**
     * This Method takes the already loaded Properties Object (All properties from config file) and
     * creates Map Object with Kafka Direct stream specific properties as Key Value pairs
     * @param properties Object with all the properties from config file.
     * @return Returns Map Object with only Kafka Direct Stream specific properties.
     */
    public static Map<String, String> getKafkaStreamProperties(Properties properties) {

        Map<String, String> kafkaParams = new HashMap<>();

        kafkaParams.put("metadata.broker.list", properties.getProperty("metadata.broker.list"));
        kafkaParams.put("schema.registry.url", properties.getProperty("schema.registry.url"));
        kafkaParams.put("group.id", properties.getProperty("group.id"));
        kafkaParams.put("specific.avro.reader", properties.getProperty("specific.avro.reader"));
        kafkaParams.put("value.deserializer", properties.getProperty("value.deserializer"));
        kafkaParams.put("key.deserializer", properties.getProperty("key.deserializer"));
        kafkaParams.put("auto.offset.reset", properties.getProperty("auto.offset.reset"));
        kafkaParams.put("enable.auto.commit", properties.getProperty("enable.auto.commit"));
        kafkaParams.put("max_rate_per_partition", properties.getProperty("max_rate_per_partition"));

        return kafkaParams;
    }

}
