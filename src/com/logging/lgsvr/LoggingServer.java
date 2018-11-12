package com.logging.lgsvr;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONObject;

import com.Common;
import com.Result;
import com.logging.lgsvr.db.LoggingDbAdaptor;
import com.logging.lgsvr.db.LoggingDbAdaptor.SingleLog;
import com.net.TcpPoint;

public class LoggingServer implements Runnable {

	/*传入连接*/
	TcpPoint _tcp;
	
	/*数据库伴随线程*/
	LoggingDbAdaptor _Adaptor;

	public static int DEFAULT_PORT = 9200;
	public static int DEFAULT_BUFFER = 8096;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor) {
		_tcp = new TcpPoint(Sock);
		_Adaptor = Adaptor;
	}

	@Override
	public void run() {
		/*默认4K缓存*/
		int buffer_len = DEFAULT_BUFFER;
		byte[] buffer = new byte[buffer_len];
		ArrayList<Byte> bytes = new ArrayList<Byte>();
		while (true) {
			try {
				Result r = _tcp.Recv(bytes);
				if (r.equals(Result.Error) || bytes.size() < 1) {
					Common.PrintException("接收错误，" + r.Message);
					continue;
				}
				if (bytes.size() > buffer_len) {
					buffer_len = bytes.size() * 2;
					buffer = new byte[buffer_len];
				}
				for (int i=0; i<bytes.size(); ++i) {
					buffer[i] = bytes.get(i);
				}
				String text = new String(buffer, 0, bytes.size(), "UTF-8");
				SingleLog log = LoggingDbAdaptor.SingleLog.CreateLog(text);
				_Adaptor.InsertLog(log);
				/*清空旧数据*/
				bytes.clear();
			} catch (Exception e) {
				Common.PrintException(e);
			}
		}
	}

	public static void main(String[] args) {
		int port = DEFAULT_PORT;
		LoggingDbAdaptor adaptor = null;
		ExecutorService es = Executors.newCachedThreadPool();
		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();
			InputStream is = Class.forName(traces[1].getClassName()).getResource("port.json").openStream();
			JSONObject ob = Common.LoadJSONObject(is);
			if (ob.has("Port")) {
				port = ob.getInt("Port");
			}
			/*服务socket永远不会退出*/
			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(port);
			/*执行数据库伴随线程*/
			adaptor = new LoggingDbAdaptor();
			es.execute(adaptor);
			System.out.println("日志服务器启动，在端口" + port + "监听。");
			while (true) {
				Socket client = ss.accept();
				client.setOOBInline(false);
				es.execute(new LoggingServer(client, adaptor));
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
}
