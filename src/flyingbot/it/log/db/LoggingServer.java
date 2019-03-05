package flyingbot.it.log.db;

import flyingbot.it.data.log.SingleLog;
import flyingbot.it.log.db.db.LoggingDbAdaptor;
import flyingbot.it.net.tcp.SocketDuplex;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LoggingServer extends SocketDuplex {

	LoggingDbAdaptor _Adaptor;

	public static int DEFAULT_PORT = 9201;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor) {
		super(Sock);
		_Adaptor = Adaptor;
	}

	private static void _LogSelf(String msg, LoggingDbAdaptor adaptor) {

		SingleLog log = new SingleLog();
		log.TimeStamp = Common.GetTimestamp();
		log.Level = "INFO";
		log.LineNumber = 0;
		log.LoggerName = "LoggerService";
		log.Message = msg;
		log.Millis = System.currentTimeMillis();
		log.SourceClassName = "LoggingServer";
		log.SourceMethodName = "OnDisconnect";
		adaptor.InsertLog(log);
	}

	@Override
	public void OnStream(byte[] Data) {
		try {
			String text = new String(Data, 0, Data.length, "UTF-8");
			SingleLog log = LoggingDbAdaptor.CreateLog(text);
			_Adaptor.InsertLog(log);
		} catch (Exception e) {
			Common.PrintException(e);
		}	
	}

	@Override
	public void OnDisconnect() {
		InetSocketAddress addr = (InetSocketAddress)this.GetSocketAddress();
		_LogSelf("Client disconnected, " + addr.getHostString() + ":" + addr.getPort(), _Adaptor);
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

		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();

			InputStream is1 = Class.forName(traces[1].getClassName()).getResource("port.json").openStream();
			JSONObject ob = Common.LoadJSONObject(is1);
			if (ob.has("Port")) {
				port = ob.getInt("Port");
			}

			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(port);

			// get singleton adaptor
			adaptor = LoggingDbAdaptor.CreateSingleton();
			Common.GetSingletonExecSvc().execute(adaptor);

			System.out.println("LOGDB is listening on port: " + port);
			_LogSelf("LOGDB is listening on port: " + port, adaptor);

			while (true) {
				Socket client = ss.accept();

				// get client address
				InetSocketAddress addr = (InetSocketAddress)client.getRemoteSocketAddress();
				String remoteIP = addr.getAddress().getHostAddress();

				InputStream is0 = null;
				try {
					is0 = Class.forName(traces[1].getClassName()).getResource("ip.json").openStream();
				}
				catch (IOException ex) {
					Common.PrintException(ex);
				}

				// validate IP
				if (!Common.VerifyIP(remoteIP, is0)) {
					_LogSelf("Invalid remote IP: " + remoteIP, adaptor);
					client.close();
					continue;
				}

				// set OOB
				client.setOOBInline(false);

				// add logging service instance
				_lock.writeLock().lock();
				servers.add(new LoggingServer(client, adaptor));
				_lock.writeLock().unlock();

				// remove dead service
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

	@Override
	public void OnConnect() {
		InetSocketAddress addr = (InetSocketAddress) this.GetSocketAddress();
		_LogSelf("Client connected, " + addr.getHostString() + ":" + addr.getPort(), _Adaptor);

		Thread.currentThread().setName("Client session (" + addr.getHostString() + ":" + addr.getPort() + ")");
	}
}
