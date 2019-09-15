package com.sky.playevents.producers;
/**
 * This is the Driver class for publishing the PlayEvents to Kafka from a CSV file (PlayEventsInputData.csv)
 * located in the /src/main/resources folder. These messages are later consumed by Kafka-Spark Direct stream.
 *
 * @author Sunil Sarda
 * @version 1.0
 * @since   2019-09-13
 */

import com.sky.playevents.configuration.ReadPropertyFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PlayEventProducer {

    static Logger logger = Logger.getLogger(PlayEventProducer.class.getName());
    static Properties properties = ReadPropertyFile.loadProperties();

    private static final String INPUT_CSV_FILE_PATH = PlayEventProducer
            .class
            .getClassLoader()
            .getResource("PlayEventsInputData.csv")
            .getPath();

    /**
     * Main method to start the program
     * @param args
     */
    public static void main(String[] args) {

        try (Reader reader = Files.newBufferedReader(Paths.get(INPUT_CSV_FILE_PATH))) {

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);

            for (CSVRecord csvRecord : csvParser) {
                PlayoutEventRecord eventRecord = new PlayoutEventRecord(Long.parseLong(csvRecord.get(0)),
                        csvRecord.get(1),csvRecord.get(2),csvRecord.get(3),csvRecord.get(4));
                publishEventRecord(eventRecord);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IO Exception in Input CSV file : " + e.toString());
        }
    }

    /**
     * This method takes the PlayoutEventRecord Object of a playEvent and sends to Kafka Broker using Fire and Forget
     * approach. This approach was used because the data is not so critical and we can afford to loose some records
     * as a trade off for performance.
     * @param eventRecord
     */
    public static void publishEventRecord(PlayoutEventRecord eventRecord) {

        Producer<String, PlayoutEventRecord> playEventProducer = new KafkaProducer<>(ReadPropertyFile
                .getKafkaProducerProperties(properties));

        try {
            playEventProducer.send(new ProducerRecord<>(properties.getProperty("playEventTopic"),
                    eventRecord.userId.toString(), eventRecord)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Interrupted Exception in Play Event Producer : " + e.toString());
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution Exception in Play Event Producer : " + e.toString());
        } finally {
            playEventProducer.close();
        }
    }

}
