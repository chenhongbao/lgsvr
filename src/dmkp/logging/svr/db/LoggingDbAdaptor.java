package dmkp.logging.svr.db;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;

import dmkp.common.util.Common;
import flyingbot.data.log.SingleLog;

public class LoggingDbAdaptor implements Runnable {

	/*从资源文件JSON中读取数据库登陆信息*/
	String _URL, _Username, _Password, _Table, _ConnStr, _InsertSQL;

	/*数据库和执行语句*/
	Connection _DbConnection = null;
	PreparedStatement _Statement = null;

	/*循环是否退出*/
	AtomicBoolean _Stopped;

	/*待写入数据库的日志*/
	ConcurrentLinkedQueue<SingleLog> _LogQueue;

	/*每隔若干日志写数据库一次，避免过大内存消耗*/
	public static long _QueryPerBatch = 500;
	
	/*上次操作数据库的毫秒数，超过一定时间自动重新连接数据库*/
	long _LastAccessDB = 0;
	public static long _ReconnectMillis = 1000 * 60 * 60;
	
	/*单件*/
	static LoggingDbAdaptor _Adaptor;
	static {
		try {
			_Adaptor = new LoggingDbAdaptor();
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
	
	/**
	 * 从JSON文本创建日志对象
	 * @param text JSON文本
	 * @return {@link SingleLog} 从JSON文本创建的对象
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
		// 设置线程名称，便于识别
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
		/*检查上次数据库操作时间，如果可能超时则重连数据库*/
		long cur = System.currentTimeMillis();
		if (cur - _LastAccessDB > _ReconnectMillis) {
			_ResetDatabase();
			_LastAccessDB = cur;
			return;
		}
	}
	
	private void _ResetDatabase() throws SQLException {
		// 数据库未初始化
		if (_DbConnection == null || _DbConnection.isClosed()) {
			_InitDatabase();
			return;
		}
		
		// 重置连接
		if (_Statement != null && !_Statement.isClosed())
		{
			_Statement.close();
		}
		if (_DbConnection.isValid(1)) {
			_DbConnection.close();
		}
		
		// 强行释放掉上次连接地引用，强迫JVM GC
		_Statement = null;
		_DbConnection = null;
		_InitDatabase();
	}

	private void _InitDatabase() throws SQLException {
		_DbConnection = DriverManager.getConnection(_ConnStr, _Username, _Password);
		/*设置事务处理*/
		if (_DbConnection.getAutoCommit()) {
			_DbConnection.setAutoCommit(false);
		}
		/*准备插入语句*/
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
			/*
			 * 如果数量超过若干，则提交以避免内存过度消耗 
			 */
			if (++count % _QueryPerBatch == 0) {
				_Statement.executeBatch();
				_DbConnection.commit();
				
				// 计数清零
				count = 0;
			}
		}
		/* 执行剩下的语句 */
		if (count > 0) {
			_Statement.executeBatch();
			_DbConnection.commit();
			count = 0;
		}
		
		// 更新操作数据库的时间
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
