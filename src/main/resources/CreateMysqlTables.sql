create database playEventDB;
use playEventDB;

DROP TABLE IF EXISTS `play_events`;
CREATE TABLE `play_events` (
`eventTimestamp` bigint,
`sessionId` VARCHAR(20),
`event` VARCHAR(20),
`userId` VARCHAR(20),
`contentId` VARCHAR(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

DROP TABLE IF EXISTS `watched_content`;
CREATE TABLE `watched_content` (
`startTimestamp` bigint,
`stopTimestamp` bigint,
`userId` VARCHAR(20),
`contentId` VARCHAR(20),
`timeWatched` bigint
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

