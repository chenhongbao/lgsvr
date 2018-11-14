package dmkp.logging.handler;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.json.JSONObject;

import dmkp.common.util.Common;
import dmkp.common.util.Result;
import dmkp.common.net.TcpPoint;

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
	
	private TcpPoint _tcp;

	public JSONSocketHandler() {
	}

	public JSONSocketHandler(String host, int port) {
		_tcp = new TcpPoint();
		Result r = _tcp.Connect(host, port);
		if (r.equals(Result.Error)) {
			Common.PrintException(new Exception("������־����" + r.Message));
		}
		setFormatter(new JSONFormatter());
		try {
			setEncoding("UTF-8");
		} catch (Exception e) {
			Common.PrintException(new Exception("������־�������" + e.getMessage()));
		}
		setErrorManager(new JSONErrorManager());
	}

	@Override
	public void publish(LogRecord record) {
		if (record == null) {
			return;
		}
		String msg = getFormatter().format(record);
		Result r = _tcp.Send(msg.getBytes(Charset.forName(getEncoding())));
		if (r.equals(Result.Error)) {
			Common.PrintException(new Exception("������־����" + r.Message));
		}
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() throws SecurityException {
		try {
			_tcp.Close();
		} catch (IOException e) {
			Common.PrintException(new Exception("�ر���־�������ӳ���" + e.getMessage()));
		}
	}
}
