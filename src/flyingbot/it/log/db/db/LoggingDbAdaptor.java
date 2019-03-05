package flyingbot.it.log.db.db;

import flyingbot.it.data.log.SingleLog;
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

public class LoggingDbAdaptor implements Runnable {

	String _URL, _Username, _Password, _Table, _ConnStr, _InsertSQL;

	Connection _DbConnection = null;
	PreparedStatement _Statement = null;

	AtomicBoolean _Stopped;

	ConcurrentLinkedQueue<SingleLog> _LogQueue;

	// do some inserts in a batch
	public static long _QueryPerBatch = 500;
	// singleton
	static LoggingDbAdaptor _Adaptor;
	public static long _ReconnectMillis = 1000 * 60 * 60;
	// timestamp last access db
	long _LastAccessDB = 0;
	static {
		try {
			_Adaptor = new LoggingDbAdaptor();
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}

	/**
	 * Parse logging from JSON text.
	 * @param text JSON text
	 * @return Log object
	 */
	public static SingleLog CreateLog(String text) {
		SingleLog log = SingleLog.Parse(new JSONObject(text));
		if (log == null) {
			Common.PrintException("Parsing log from JSON failed.");
			log = new SingleLog();
		}
		return log;
	}
	
	public static LoggingDbAdaptor CreateSingleton() {
		return _Adaptor;
	}

	protected LoggingDbAdaptor() throws Exception {
		_Stopped = new AtomicBoolean(true);
		_LogQueue = new ConcurrentLinkedQueue<SingleLog>();
		_LoadConfiguration();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Database deamon");
		
		_Stopped.set(false);
		while (!_Stopped.get()) {
			try {
				_WriteLogs(_LogQueue);
				Thread.sleep(1000);
			} catch (Exception e) {
				Common.PrintException(e);
			}
		}
	}

	public void InsertLog(SingleLog Log) {
		_LogQueue.add(Log);
	}

	public void Stop() {
		_Stopped.set(true);
	}

	public boolean IsStopped() {
		return _Stopped.get();
	}

	private void _LoadConfiguration() throws Exception {
		InputStream is = this.getClass().getResource("dblogin.json").openStream();
		JSONObject obj = Common.LoadJSONObject(is);
		if (obj.has("URL") && obj.has("Username") && obj.has("Password")) {
			_URL = obj.getString("URL");
			_Username = obj.getString("Username");
			_Password = obj.getString("Password");
			_Table = obj.getString("Table");
			_ConnStr = _URL
					+ "?characterEncoding=utf8&useSSL=false"
					+ "&serverTimezone=UTC&rewriteBatchedStatements=true";
		}
		Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
	}
	
	private void _ConnectDatabase() throws SQLException {
		long cur = System.currentTimeMillis();
		if (cur - _LastAccessDB > _ReconnectMillis) {
			_ResetDatabase();
			_LastAccessDB = cur;
			return;
		}
	}
	
	private void _ResetDatabase() throws SQLException {
		if (_DbConnection == null || _DbConnection.isClosed()) {
			_InitDatabase();
			return;
		}

		if (_Statement != null && !_Statement.isClosed())
		{
			_Statement.close();
		}
		if (_DbConnection.isValid(1)) {
			_DbConnection.close();
		}

		_Statement = null;
		_DbConnection = null;
		_InitDatabase();
	}

	private void _InitDatabase() throws SQLException {
		_DbConnection = DriverManager.getConnection(_ConnStr, _Username, _Password);

		// turn off auto-commit
		if (_DbConnection.getAutoCommit()) {
			_DbConnection.setAutoCommit(false);
		}

		// build sql string
		_InsertSQL = "INSERT INTO `" + _Table + "` values (?,?,?,?,?,?,?,?)";
		_Statement = _DbConnection.prepareStatement(_InsertSQL);

	}

	private void _WriteLogs(Queue<SingleLog> Logs) throws Exception {
		long count = 0;
		if (Logs.size() < 1) {
			return;
		}
		_ConnectDatabase();
		
		while (Logs.size() > 0) {
			SingleLog log = Logs.poll();
			if (log == null) {
				continue;
			}
			_Statement.setString(1,  log.TimeStamp);
			_Statement.setString(2, log.Level);
			_Statement.setString(3, log.LoggerName);
			_Statement.setString(4, log.Message);
			_Statement.setLong(5, log.Millis);
			_Statement.setString(6, log.SourceClassName);
			_Statement.setString(7, log.SourceMethodName);
			_Statement.setInt(8, log.LineNumber);
			_Statement.addBatch();

			// some inserts at a time
			if (++count % _QueryPerBatch == 0) {
				_Statement.executeBatch();
				_DbConnection.commit();

				// reset counter
				count = 0;
			}
		}

		// process the remaining inserts
		if (count > 0) {
			_Statement.executeBatch();
			_DbConnection.commit();
			count = 0;
		}

		// update timestamp
		_LastAccessDB = System.currentTimeMillis();
	}

	@Override
	protected void finalize() {
		try {
			if (!_DbConnection.isClosed()) {
				_DbConnection.close();
			}
			if (!_Statement.isClosed()) {
				_Statement.close();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
}
