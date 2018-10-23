package com.logging.lgsvr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONObject;

import com.logging.lgsvr.db.LoggingDbAdaptor;
import com.logging.lgsvr.db.LoggingDbAdaptor.SingleLog;

public class LoggingServer implements Runnable {

	/*传入连接*/
	Socket _Sock;
	
	/*数据库伴随线程*/
	LoggingDbAdaptor _Adaptor;

	public static int MAX_PACKCT_LEN = 1500;
	public static int DEFAULT_PORT = 9200;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor) {
		_Sock = Sock;
		_Adaptor = Adaptor;
	}

	@Override
	public void run() {
		int cnt = 0;
		byte[] buffer = new byte[MAX_PACKCT_LEN];
		InputStream is = null;
		try {
			is = _Sock.getInputStream();
		} catch (IOException e) {
			Common.PrintException(e);
			return;
		}
		while (true) {
			try {
				cnt = is.read(buffer);
				if (cnt < MAX_PACKCT_LEN) {
					String text = new String(buffer, 0, cnt, "UTF-8");
					SingleLog log = LoggingDbAdaptor.SingleLog.CreateLog(text);
					_Adaptor.InsertLog(log);
				} else {
					Common.PrintException(new Exception("Log overflow"));
				}
			} catch (Exception e) {
				Common.PrintException(e);
			}
		}
	}

	public static void main(String[] args) {
		int port = DEFAULT_PORT;
		String file = null;
		LoggingDbAdaptor adaptor = null;
		ExecutorService es = Executors.newCachedThreadPool();
		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();
			file = Class.forName(traces[1].getClassName()).getResource("port.json").getFile();
			BufferedReader br = new BufferedReader(
					new FileReader(new File(file)));
			String line = null;
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			br.close();
			JSONObject ob = new JSONObject(sb.toString());
			if (ob.has("Port")) {
				port = ob.getInt("Port");
			}
			/*服务socket永远不会退出*/
			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(port);
			/*执行数据库伴随线程*/
			adaptor = new LoggingDbAdaptor();
			es.execute(adaptor);
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
