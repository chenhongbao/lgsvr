package com.logging.svr.db;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;

import com.Common;

public class LoggingDbAdaptor implements Runnable {

	public static class SingleLog {
		public String Level = "";
		public String LoggerName = "";
		public String Message = "";
		public long Millis = -1;
		public String SourceClassName = "";
		public String SourceMethodName = "";

		SingleLog() {
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
				if (obj.has("Level") && obj.has("LoggerName") 
						&& obj.has("Message") && obj.has("Millis") 
						&& obj.has("SourceClassName") && obj.has("SourceMethodName")) {
					log = new SingleLog();
					log.Level = obj.getString("Level");
					log.LoggerName = obj.getString("LoggerName");
					log.Message = obj.getString("Message");
					log.Millis = obj.getLong("Millis");
					log.SourceClassName = obj.getString("SourceClassName");
					log.SourceMethodName = obj.getString("SourceMethodName");
				}
				return log;
			} catch (Exception e) {
				Common.PrintException(e);
				return log;
			}
		}
	}

	/*����Դ�ļ�JSON�ж�ȡ���ݿ��½��Ϣ*/
	String _URL, _Username, _Password, _ConnStr;

	/*���ݿ��ִ�����*/
	Connection _DbConnection = null;
	PreparedStatement _Statement = null;

	/*ѭ���Ƿ��˳�*/
	AtomicBoolean _Stopped;

	/*��д�����ݿ����־*/
	ConcurrentLinkedQueue<SingleLog> _LogQueue;

	/*ÿ��������־д���ݿ�һ�Σ���������ڴ�����*/
	public static long _QueryPerBatch = 500;
	public static String _InsertSql = "INSERT INTO `loggingdb`.`log_ws_01` values (?,?,?,?,?,?)";

	public LoggingDbAdaptor() throws Exception {
		_Stopped = new AtomicBoolean(true);
		_LogQueue = new ConcurrentLinkedQueue<SingleLog>();
		_LoadConfiguration();
	}

	@Override
	public void run() {
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
			_ConnStr = _URL
					+ "?characterEncoding=utf8&useSSL=false"
					+ "&serverTimezone=UTC&rewriteBatchedStatements=true";
		}
		Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
	}

	private void _ConnectDatabase() throws Exception {
		if (_DbConnection == null || _DbConnection.isClosed()) {
			_DbConnection = DriverManager.getConnection(_ConnStr, _Username, _Password);
		}
		if (!_DbConnection.isValid(5)) {
			_DbConnection.close();
			_DbConnection = DriverManager.getConnection(_ConnStr, _Username, _Password);
		}
		if (_Statement == null || _Statement.isClosed()) {
			_Statement = _DbConnection.prepareStatement(_InsertSql);
		}
		if (_DbConnection.getAutoCommit()) {
			_DbConnection.setAutoCommit(false);
		}
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
			_Statement.setString(1, log.Level);
			_Statement.setString(2, log.LoggerName);
			_Statement.setString(3, log.Message);
			_Statement.setLong(4, log.Millis);
			_Statement.setString(5, log.SourceClassName);
			_Statement.setString(6, log.SourceMethodName);
			_Statement.addBatch();
			/* ��������������ɣ����ύ�Ա����ڴ�������� */
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
		}
	}
}
