package com.sky.playevents.consumerstream;

/**
 * This Interface has only static methods needed to perform the SQL transactions.
 *
 * @author Sunil Sarda
 * @version 1.0
 * @since   2019-09-13
 */

import com.sky.playevents.producers.PlayoutEventRecord;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SqlTransactions {

    Connection conn = ConnectionPool.getConnectionFromPool();

    /**
     * This method takes the Play Event record and inserts into the MySql database table playEventDB:play_events
     * @param record PlayoutEventRecord object
     * @throws SQLException
     */
    static void insertPlayEventRecord(PlayoutEventRecord record) throws SQLException {
        String insertQuery = "INSERT INTO play_events VALUES (?, ?, ?, ?, ?)";
        PreparedStatement preparedStmtInsert = conn.prepareStatement(insertQuery);

        preparedStmtInsert.setLong(1, record.eventTimestamp);
        preparedStmtInsert.setString(2, record.sessionId.toString());
        preparedStmtInsert.setString(3, record.event.toString());
        preparedStmtInsert.setString(4, record.userId.toString());
        preparedStmtInsert.setString(5, record.contentId.toString());
        preparedStmtInsert.execute();
        conn.commit();
    }

    /**
     * This method fetches the event start record of an event for a specific Session id to calculate the Content
     * watched time. The query Orders the records by EventTime in descending order and takes the 1st one to calculate
     * the watched time.
     * @param sessionID
     * @return Returns the row from playEventDB:play_events
     * @throws SQLException
     */
    static ResultSet getPlayEventStartRecord(String sessionID) throws SQLException {
        String selectQuery = "SELECT eventTimestamp,userId,contentId FROM play_events WHERE " +
                "sessionId = ? AND event = 'Start' ORDER BY eventTimestamp DESC";

        PreparedStatement preparedStmtSelect = conn.prepareStatement(selectQuery);
        preparedStmtSelect.setString(1, sessionID);
        ResultSet resultSet = preparedStmtSelect.executeQuery();
        return resultSet;
    }

    /**
     * This method inserts the record into playEventDB:watched_content table after calculating the watched time.
     * @param eventStartTime
     * @param eventStopTime
     * @param userId
     * @param contentId
     * @param timeWatched
     * @throws SQLException
     */
    static void insertContentWatchedRecord(Long eventStartTime, Long eventStopTime, String userId,
                                           String contentId, Long timeWatched) throws SQLException {
        String insertQuery = "INSERT INTO watched_content VALUES (?, ?, ?, ?, ?)";
        PreparedStatement preparedStmtInsert = conn.prepareStatement(insertQuery);

        preparedStmtInsert.setLong(1, eventStartTime);
        preparedStmtInsert.setLong(2, eventStopTime);
        preparedStmtInsert.setString(3, userId);
        preparedStmtInsert.setString(4, contentId);
        preparedStmtInsert.setLong(5, timeWatched);
        preparedStmtInsert.execute();
        conn.commit();
    }
}
