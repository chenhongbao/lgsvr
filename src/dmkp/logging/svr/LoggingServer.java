package dmkp.logging.svr;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONObject;

import dmkp.common.net.Duplex;
import dmkp.common.util.Common;
import dmkp.common.util.Result;
import dmkp.logging.svr.db.LoggingDbAdaptor;
import dmkp.logging.svr.db.LoggingDbAdaptor.SingleLog;

public class LoggingServer extends Duplex {

	
	/*数据库伴随线程*/
	LoggingDbAdaptor _Adaptor;

	public static int DEFAULT_PORT = 9200;
	public static int DEFAULT_BUFFER = 8096;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor) {
		super(Sock);
		_Adaptor = Adaptor;
	}
	
	@Override
	public void OnConnect() {	
	}

	@Override
	public void OnStream(byte[] Data) {
		try {
			String text = new String(Data, 0, Data.length, "UTF-8");
			SingleLog log = LoggingDbAdaptor.SingleLog.CreateLog(text);
			_Adaptor.InsertLog(log);
		} catch (Exception e) {
			Common.PrintException(e);
		}	
	}

	@Override
	public void OnDisconnect() {
	}

	@Override
	public void OnHearbeatError(Result Reason) {
	}
	
	static Set<LoggingServer> servers;
	static ReentrantReadWriteLock _lock;
	static {
		_lock = new ReentrantReadWriteLock();
		servers = new HashSet<LoggingServer>();
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
				/*同步*/
				_lock.writeLock().lock();
				servers.add(new LoggingServer(client, adaptor));
				_lock.writeLock().unlock();
				/*检查所有连接是否合法，不合法的删除*/
				Set<LoggingServer> tmp = new HashSet<LoggingServer>();
				_lock.readLock().lock();
				for (LoggingServer s : servers) {
					if (!s.IsConnected()) {
						tmp.add(s);
					}
				}
				_lock.readLock().unlock();
				_lock.writeLock().lock();
				for (LoggingServer s : tmp) {
					servers.remove(s);
				}
				_lock.writeLock().unlock();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
}
