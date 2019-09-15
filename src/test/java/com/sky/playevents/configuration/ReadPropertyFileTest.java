package com.sky.playevents.configuration;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class ReadPropertyFileTest {

    static Logger logger = Logger.getLogger(ReadPropertyFile.class.getName());
    private static ReadPropertyFileTest propertiesSingleInstance = null;

    @Test
    void testGetInstance() {
        if (propertiesSingleInstance == null) {
            propertiesSingleInstance = new ReadPropertyFileTest();
        }
        assertNotNull(propertiesSingleInstance);
    }


    @Test
    void testLoadPropertiesforValues() {

        try (InputStream input = ReadPropertyFile.class.getClassLoader().getResourceAsStream("config.properties")) {
            assertNotNull(input);
            Properties prop = new Properties();
            if (input == null) {
                logger.log(Level.SEVERE, "Sorry, unable to find config.properties file ");
            } else {
                prop.load(input);
                assertEquals("org.apache.kafka.common.serialization.StringSerializer",prop.getProperty("key.serializer"));
                assertEquals("local[*]",prop.getProperty("sparkMaster"));
                assertEquals("sky",prop.getProperty("mySqlUser"));
                assertEquals("28800",prop.getProperty("maxTimeWatched"));
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IO Exception in Config file : " + e.toString());
        }
    }


    @Test
    void testLoadPropertiesNullInput() {

        try (InputStream input = ReadPropertyFile.class.getClassLoader().getResourceAsStream("sky.properties")) {
            Properties prop = new Properties();
            if (input == null) {
                assertNull(input);
                logger.log(Level.SEVERE, "Sorry, unable to find config.properties file ");
            } else {
                prop.load(input);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IO Exception in Config file : " + e.toString());
        }
    }


    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Test
    void testLoadPropertiesIOException() {

        try (InputStream input = ReadPropertyFile.class.getClassLoader().getResourceAsStream("config.properties")) {

            Properties prop = new Properties();
            if (input == null) {
                logger.log(Level.SEVERE, "Sorry, unable to find config.properties file ");
            } else {
                //prop.load(input);
            }
        } catch (IOException e) {
            exception.expect(IOException.class);
            logger.log(Level.SEVERE, "IO Exception in Config file : " + e.toString());
        }
    }
}