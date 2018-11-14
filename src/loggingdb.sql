CREATE DATABASE loggingdb
	CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
    
CREATE TABLE log_ws_01 (
	`TimeStamp` CHAR(255) NOT NULL COMMENT 'Time stamp for the logging',
	`Level` CHAR(255) NOT NULL COMMENT 'Logging level used by java.util.logging.Logger',
    `LoggerName` CHAR(255) NOT NULL COMMENT 'Logger name used by java.utl.logging.Logger',
    `Message` TINYTEXT NOT NULL COMMENT 'Logging message sent by java.util.logging.Logger, utf-8',
    `Millis` BIGINT NOT NULL COMMENT 'Logging time in milliseconds since 1970',
    `SourceClassName` CHAR(255) NOT NULL COMMENT 'Class that sent the log',
    `SourceMethodName` CHAR(255) NOT NULL COMMENT 'Method that sent the log',
    KEY BTREEHASH(`Millis`),
    KEY HASH(`SourceClassName`, `SourceMethodName`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;