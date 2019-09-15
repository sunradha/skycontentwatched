package com.sky.playevents.consumerstream;
/**
 * This Class has methods that processes each record from the JavaPairInputDStream RDD of spark.
 *
 * @author Sunil Sarda
 * @version 1.0
 * @since   2019-09-13
 */
import com.sky.playevents.producers.*;
import com.sky.playevents.configuration.ReadPropertyFile;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

class ProcessRecords {

    private static Logger logger = Logger.getLogger(ProcessRecords.class.getName());
    private static Properties properties = ReadPropertyFile.loadProperties();

    /**
     * This is the main method called from the lambda function of the Kafka-Spark direct stream. It takes each
     * record (as a deserialized object of PlayoutEventRecord) and processes it.
     * @param record
     * @throws SQLException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    static void processRDDRecord(PlayoutEventRecord record) throws SQLException, ExecutionException, InterruptedException {

        if (record.event.toString().equals("Start")) {
            SqlTransactions.insertPlayEventRecord(record);

        } else if (record.event.toString().equals("Stop")) {
            ProcessRecords.computeWatchedTime(record);
            SqlTransactions.insertPlayEventRecord(record);
        }
    }

    /**
     * This method computes the content watched time for the event. It's called only when we receive the stop event.
     * @param record
     * @throws SQLException
     */
    static void computeWatchedTime(PlayoutEventRecord record) throws SQLException {

        ResultSet resultSet = SqlTransactions.getPlayEventStartRecord(record.sessionId.toString());

        if (resultSet.next()) {
            long eventStartTime = resultSet.getLong(1);
            String userId = resultSet.getString(2);
            String contentId = resultSet.getString(3);
            long timeWatched = record.eventTimestamp - eventStartTime;

            SqlTransactions.insertContentWatchedRecord(eventStartTime, record.eventTimestamp, userId, contentId, timeWatched);

            ContentWatchedRecord contentWatched = new ContentWatchedRecord(record.eventTimestamp,record.userId.toString(),
                    record.contentId.toString(),timeWatched);

            if ((timeWatched > Long.parseLong(properties.getProperty("minTimeWatched"))) &&
                    (timeWatched < Long.parseLong(properties.getProperty("maxTimeWatched")))) {

                ProcessRecords.publishContentWatched(contentWatched);
            }

        } else {
            logger.log(Level.WARNING, "No Event Start record for this Session ID : " + record.sessionId.toString());
        }
    }

    /**
     * This method publishes the ContentWatched record back to the kafka broker to watchedContentTopic topic.
     * This again uses Fire and Forget approach to send the record to Kafka.
     * @param contentWatched
     */
    static void publishContentWatched(ContentWatchedRecord contentWatched) {
        String watchedContentTopic = properties.getProperty("watchedContentTopic");
        String messageKey = contentWatched.userId.toString();

        Producer<String, ContentWatchedRecord> contentWatchedProducer = new KafkaProducer<>(ReadPropertyFile.loadProperties());

        try {
            contentWatchedProducer.send(new ProducerRecord<>(watchedContentTopic, messageKey, contentWatched)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Interrupted Exception in Watched Content Producer : " + e.toString());
            contentWatchedProducer.close();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Execution Exception in Watched Content Producer : " + e.toString());
            contentWatchedProducer.close();
        }
    }
}

