package com.sky.playevents.producers;

import com.sky.playevents.configuration.ReadPropertyFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;


import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class PlayEventProducerTest {

    static Logger logger = Logger.getLogger(PlayEventProducer.class.getName());
    static Properties properties = ReadPropertyFile.loadProperties();

    static final String INPUT_CSV_FILE_PATH = PlayEventProducer
            .class
            .getClassLoader()
            .getResource("PlayEventsInputData.csv")
            .getPath();


    @Test
    void testMainNullObjects() {
        try (Reader reader = Files.newBufferedReader(Paths.get(INPUT_CSV_FILE_PATH))) {
            assertNotNull(reader);

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            assertNotNull(csvParser);

            for (CSVRecord csvRecord : csvParser) {
                PlayoutEventRecord eventRecord = new PlayoutEventRecord(Long.parseLong(csvRecord.get(0)),
                        csvRecord.get(1), csvRecord.get(2), csvRecord.get(3), csvRecord.get(4));
                assertNotNull(eventRecord);
                assertNotNull(eventRecord);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IO Exception in Input CSV file : " + e.toString());
        }
    }

    @Test
    void testMainFilecheck1() throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(INPUT_CSV_FILE_PATH));
        assertEquals("/home/ssarda/contentwatch/target/classes/PlayEventsInputData.csv", INPUT_CSV_FILE_PATH);
        assertNotEquals("/PlayEventsInputData.csv", INPUT_CSV_FILE_PATH);
        assertNotEquals("", INPUT_CSV_FILE_PATH);
    }


    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Test
    void testMainIOException() {
        try (Reader reader = Files.newBufferedReader(Paths.get("PlayEventsInputData.csv"))) {
            assertNotNull(reader);

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            assertNotNull(csvParser);

            for (CSVRecord csvRecord : csvParser) {
                PlayoutEventRecord eventRecord = new PlayoutEventRecord(Long.parseLong(csvRecord.get(0)),
                        csvRecord.get(1), csvRecord.get(2), csvRecord.get(3), csvRecord.get(4));
            }
        } catch (IOException e) {
            exception.expect(IOException.class);
            logger.log(Level.SEVERE, "IO Exception in Input CSV file : " + e.toString());
        }
    }


    @Test
    void testKafkaProducerNull() {
        Producer<String, PlayoutEventRecord> eventProducer = new KafkaProducer<>(ReadPropertyFile
                .getKafkaProducerProperties(properties));
        assertNotNull(eventProducer);
    }
}