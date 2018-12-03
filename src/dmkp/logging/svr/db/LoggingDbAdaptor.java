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

public class LoggingDbAdaptor implements Runnable {

	public static class SingleLog {
		public String TimeStamp = "";
		public String Level = "";
		public String LoggerName = "";
		public String Message = "";
		public long Millis = -1;
		public String SourceClassName = "";
		public String SourceMethodName = "";
		public int LineNumber = -1;

		public SingleLog() {
		}
		
		/**
		 * ��JSON�ı�������־����
		 * @param text JSON�ı�
		 * @return {@link SingleLog} ��JSON�ı������Ķ���
		 */
		public static SingleLog CreateLog(String text) {
			SingleLog log = null;
			try {
				JSONObject obj = new JSONObject(text);
				log = new SingleLog();
				log.TimeStamp = obj.getString("TimeStamp");
				log.Level = obj.getString("Level");
				log.LoggerName = obj.getString("LoggerName");
				log.Message = obj.getString("Message");
				log.Millis = obj.getLong("Millis");
				log.SourceClassName = obj.getString("SourceClassName");
				log.SourceMethodName = obj.getString("SourceMethodName");
				log.LineNumber = obj.getInt("LineNumber");
				return log;
			} catch (Exception e) {
				Common.PrintException(e);
				return log;
			}
		}
	}

	/*����Դ�ļ�JSON�ж�ȡ���ݿ��½��Ϣ*/
	String _URL, _Username, _Password, _Table, _ConnStr, _InsertSQL;

	/*���ݿ��ִ�����*/
	Connection _DbConnection = null;
	PreparedStatement _Statement = null;

	/*ѭ���Ƿ��˳�*/
	AtomicBoolean _Stopped;

	/*��д�����ݿ����־*/
	ConcurrentLinkedQueue<SingleLog> _LogQueue;

	/*ÿ��������־д���ݿ�һ�Σ���������ڴ�����*/
	public static long _QueryPerBatch = 500;
	
	/*�ϴβ������ݿ�ĺ�����������һ��ʱ���Զ������������ݿ�*/
	long _LastAccessDB = 0;
	public static long _ReconnectMillis = 1000 * 60 * 60;
	
	/*����*/
	static LoggingDbAdaptor _Adaptor;
	static {
		try {
			_Adaptor = new LoggingDbAdaptor();
		} catch (Exception e) {
			Common.PrintException(e);
		}
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
		// �����߳����ƣ�����ʶ��
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
		/*����ϴ����ݿ����ʱ�䣬������ܳ�ʱ���������ݿ�*/
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
		else
		{
			// ǿ���ͷŵ��ϴ����ӵ����ã�ǿ��JVM GC
			_Statement = null;
			_DbConnection = null;
		}
		_InitDatabase();
	}

	private void _InitDatabase() throws SQLException {
		_DbConnection = DriverManager.getConnection(_ConnStr, _Username, _Password);
		/*����������*/
		if (_DbConnection.getAutoCommit()) {
			_DbConnection.setAutoCommit(false);
		}
		/*׼���������*/
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
			 * ��������������ɣ����ύ�Ա����ڴ�������� 
			 */
			if (++count % _QueryPerBatch == 0) {
				_Statement.executeBatch();
				_DbConnection.commit();
			}
		}
		/* ִ��ʣ�µ���� */
		_Statement.executeBatch();
		_DbConnection.commit();
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
