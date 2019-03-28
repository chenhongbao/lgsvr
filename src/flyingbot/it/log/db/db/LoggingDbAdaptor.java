package flyingbot.it.log.db.db;

import flyingbot.it.data.log.SingleLog;
import flyingbot.it.log.db.resources.Constants;
import flyingbot.it.util.Common;
import org.json.JSONObject;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static flyingbot.it.log.db.resources.Constants.*;

public class LoggingDbAdaptor implements Runnable {
	/**
	 * Singleton
	 */
	static LoggingDbAdaptor adaptor;

	static {
		try {
			adaptor = new LoggingDbAdaptor();
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}

	/**
	 * Database info
	 */
	String URL, userName, password, tableName, connStr, insertSQL;
	/**
	 * Connection instance
	 */
	Connection dbConnection = null;
	/**
	 * SQL statment
	 */
	PreparedStatement statement = null;
	/**
	 * Mark if it is stopped
	 */
	AtomicBoolean isStopped;
	/**
	 * Logging queue
	 */
	ConcurrentLinkedQueue<SingleLog> logQueue;
	/**
	 * timestamp last access db
	 */
	long lastAccessDB = 0;

	protected LoggingDbAdaptor() throws Exception {
		isStopped = new AtomicBoolean(true);
		logQueue = new ConcurrentLinkedQueue<SingleLog>();
		loadConfiguration();
	}
	
	/**
	 * Parse logging from JSON text.
	 * @param text JSON text
	 * @return Log object
	 */
	public static SingleLog createLog(String text) {
		SingleLog log = SingleLog.Parse(new JSONObject(text));
		if (log == null) {
			Common.PrintException("Parsing log from JSON failed.");
			log = new SingleLog();
		}
		return log;
	}

    public static LoggingDbAdaptor singleton() {
		return adaptor;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Database deamon");

		isStopped.set(false);
		while (!isStopped.get()) {
			try {
				writeLogs(logQueue);
				Thread.sleep(QueueScanMillis);
			} catch (Exception e) {
				Common.PrintException(e);
			}
		}
	}

	public void insertLog(SingleLog Log) {
		logQueue.add(Log);
	}

	public void stop() {
		isStopped.set(true);
	}

	public boolean isStopped() {
		return isStopped.get();
	}

	private void loadConfiguration() throws Exception {
		InputStream is = Constants.class.getResource("dblogin.json").openStream();
		JSONObject obj = Common.LoadJSONObject(is);
		if (obj.has(ConfigTag_URL) && obj.has(ConfigTag_User) && obj.has(ConfigTag_Pwd)) {
			URL = obj.getString(ConfigTag_URL);
			userName = obj.getString(ConfigTag_User);
			password = obj.getString(ConfigTag_Pwd);
			tableName = obj.getString(ConfigTag_Table);
			connStr = URL
					+ "?characterEncoding=utf8&useSSL=false"
					+ "&serverTimezone=UTC&rewriteBatchedStatements=true";
		}
		Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
	}

	private void connectDatabase() throws SQLException {
		long cur = System.currentTimeMillis();
		if (cur - lastAccessDB > reconnectMillis) {
			resetDatabase();
			lastAccessDB = cur;
			return;
		}
	}

	private void resetDatabase() throws SQLException {
		if (dbConnection == null || dbConnection.isClosed()) {
			initDatabase();
			return;
		}

		if (statement != null && !statement.isClosed()) {
			statement.close();
		}
		if (dbConnection.isValid(1)) {
			dbConnection.close();
		}

		statement = null;
		dbConnection = null;
		initDatabase();
	}

	private void initDatabase() throws SQLException {
		dbConnection = DriverManager.getConnection(connStr, userName, password);

		// turn off auto-commit
		if (dbConnection.getAutoCommit()) {
			dbConnection.setAutoCommit(false);
		}

		// build sql string
		insertSQL = "INSERT INTO `" + tableName + "` values (?,?,?,?,?,?,?,?)";
		statement = dbConnection.prepareStatement(insertSQL);

	}

	private void writeLogs(Queue<SingleLog> Logs) throws Exception {
		long count = 0;
		if (Logs.size() < 1) {
			return;
		}
		connectDatabase();
		
		while (Logs.size() > 0) {
			SingleLog log = Logs.poll();
			if (log == null) {
				continue;
			}
			statement.setString(1, log.TimeStamp);
			statement.setString(2, log.Level);
			statement.setString(3, log.LoggerName);
			statement.setString(4, log.Message);
			statement.setLong(5, log.Millis);
			statement.setString(6, log.SourceClassName);
			statement.setString(7, log.SourceMethodName);
			statement.setInt(8, log.LineNumber);
			statement.addBatch();

			// some inserts at a time
			if (++count % queryPerBatch == 0) {
				statement.executeBatch();
				dbConnection.commit();

				// reset counter
				count = 0;
			}
		}

		// process the remaining inserts
		if (count > 0) {
			statement.executeBatch();
			dbConnection.commit();
			count = 0;
		}

		// update timestamp
		lastAccessDB = System.currentTimeMillis();
	}

	@Override
	protected void finalize() {
		try {
			if (!dbConnection.isClosed()) {
				dbConnection.close();
			}
			if (!statement.isClosed()) {
				statement.close();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
}
