CREATE DATABASE loggingdb
	CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
    
CREATE TABLE log_ws_01 (
	`TimeStamp` CHAR(255) COMMENT 'Time stamp for the logging',
	`Level` CHAR(255) COMMENT 'Logging level used by java.util.logging.Logger',
    `LoggerName` CHAR(255) COMMENT 'Logger name used by java.utl.logging.Logger',
    `Message` TINYTEXT COMMENT 'Logging message sent by java.util.logging.Logger, utf-8',
    `Millis` BIGINT COMMENT 'Logging time in milliseconds since 1970',
    `SourceClassName` CHAR(255) COMMENT 'Class that sent the log',
    `SourceMethodName` CHAR(255) COMMENT 'Method that sent the log'
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;