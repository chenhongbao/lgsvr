package dmkp.logging.logger.handler;

import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.json.JSONObject;

import dmkp.common.util.Common;
import dmkp.common.util.Result;
import dmkp.common.net.SocketDuplex;

public class JSONSocketHandler extends Handler {

	class JSONFormatter extends Formatter {
		
		String _JsonText = null;

		public JSONFormatter() {
		}

		@Override
		public String format(LogRecord record) {
			return GetJsonText(record);
		}
	
		protected String GetJsonText(LogRecord record) {
			JSONObject obj = new JSONObject();
			obj.put("TimeStamp", Common.GetTimestamp());
			obj.put("Level", record.getLevel().getName());
			obj.put("LoggerName", record.getLoggerName());
			obj.put("Message", record.getMessage());
			obj.put("Millis", record.getMillis());
			obj.put("SourceClassName", record.getSourceClassName());
			obj.put("SourceMethodName", record.getSourceMethodName());
			/*
			 * 因为获取行号需要JVM的栈信息，而从LogRecord传递行号则需要构造Throwable对象，开销非常大。
			 * 所以这里默认提供零。
			 */
			obj.put("LineNumber", 0);
			return obj.toString(0);
		}
	}
	
	class JSONErrorManager extends ErrorManager {
		
		public JSONErrorManager() {
		}
		
		@Override
		public synchronized void error(String msg, Exception ex, int code) {
			File f = new File("exception.log");
			try {
				if (!f.exists()) {
					f.createNewFile();
				}
				ex.printStackTrace(new PrintStream(f));
			} catch (Exception e1) {}
		}
		
	}
	
	class SocketDuplexConn extends SocketDuplex {

		@Override
		public void OnConnect() {		
		}

		@Override
		public void OnStream(byte[] Data) {
		}

		@Override
		public void OnDisconnect() {
			InetSocketAddress sa = (InetSocketAddress)GetSocketAddress();
			Common.PrintException("日志服务器连接断开，" + sa.getAddress().getCanonicalHostName() + ":" + sa.getPort());
		}

		@Override
		public void OnHearbeatError(Result Reason) {
		}
	}
	
	private SocketDuplex _tcp;
	
	// 连接信息
	private String _host;
	private int _port;

	public JSONSocketHandler() {
	}

	public JSONSocketHandler(String host, int port) {
		_tcp = new SocketDuplexConn();
		
		// 等到发日志才连接，延迟连接
		_host = host;
		_port = port;
		
		// 设置formatter
		setFormatter(new JSONFormatter());
		try {
			setEncoding("UTF-8");
		} catch (Exception e) {
			Common.PrintException(new Exception("设置日志编码出错，" + e.getMessage()));
		}
		setErrorManager(new JSONErrorManager());
	}

	@Override
	public void publish(LogRecord record) {
		if (record == null) {
			return;
		}
		String msg = getFormatter().format(record);
		
		// 连接日志服务器
		if (!_tcp.IsConnected()) {
			Result r = _tcp.Connect(_host, _port);
			if (r.equals(Result.Error)) {
				Common.PrintException(new Exception("连接日志出错，" + r.Message + "，" + msg));
				return;
			}
		}
		
		// 发送日志
		Result r = _tcp.SendStream(msg.getBytes(Charset.forName(getEncoding())));
		if (r.equals(Result.Error)) {
			Common.PrintException(new Exception("发送日志出错，" + r.Message));
		}
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() throws SecurityException {
		Result r = _tcp.Disconnect();
		if (r.equals(Result.Error)) {
			Common.PrintException(new Exception("关闭日志网络连接出错，" + r.Message));
		}
	}
}
