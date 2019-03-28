package flyingbot.it.log.db;

import flyingbot.it.data.log.SingleLog;
import flyingbot.it.log.db.db.LoggingDbAdaptor;
import flyingbot.it.log.db.resources.Constants;
import flyingbot.it.log.db.ws.LogSubscriber;
import flyingbot.it.log.db.ws.WsLogServer;
import flyingbot.it.net.tcp.SocketDuplex;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static flyingbot.it.log.db.resources.Constants.*;

public class LoggingServer extends SocketDuplex {
	static ReentrantReadWriteLock rwLock;

	static {
		rwLock = new ReentrantReadWriteLock();
		servers = new HashSet<LoggingServer>();
	}

	/**
	 * Adaptor for writing logs to database
	 */
	LoggingDbAdaptor adaptor;
	/**
	 * Observers listening on WS
	 */
	LogSubscriber subscriber;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor, LogSubscriber subs) {
		super(Sock);
		this.adaptor = Adaptor;
		this.subscriber = subs;
	}

	public static void info(String msg) {
		thisLog("INFO", msg);
	}

	public static void warn(String msg) {
		thisLog("WARNING", msg);
	}

	static void thisLog(String level, String msg) {
		SingleLog log = new SingleLog();
		log.TimeStamp = Common.GetTimestamp();
		log.Level = level;
		log.LineNumber = 0;
		log.LoggerName = "LoggerService";
		log.Message = msg;
		log.Millis = System.currentTimeMillis();
		log.SourceClassName = "LoggingServer";
		log.SourceMethodName = "OnDisconnect";

        // insert and broadcast to observers
        LoggingDbAdaptor.singleton().insertLog(log);
        LogSubscriber.singleton().distributeLog(log);
	}

	@Override
	public void OnHearbeatError(Result Reason) {
	}
	
	static Set<LoggingServer> servers;

	public static void main(String[] args) {
		int clientPort = InsideListenPort;
		int observerPort = ObserverListenPort;

		// Write log to DB
        final LoggingDbAdaptor adaptor = LoggingDbAdaptor.singleton();

		// Subscribers
		final LogSubscriber subs = LogSubscriber.singleton();

		// Serve observers via WebSocket
        final WsLogServer wsSvr = WsLogServer.instance(observerPort, subs);

		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();

			// Get port for clients
			InputStream is1 = Constants.class.getResource("port.json").openStream();
			JSONObject ob = Common.LoadJSONObject(is1);
			if (ob.has(ConfigTag_Port)) {
				clientPort = ob.getInt(ConfigTag_Port);
			}

			// Get port for observers
			is1 = Constants.class.getResource("ws_port.json").openStream();
			ob = Common.LoadJSONObject(is1);
			if (ob.has(ConfigTag_Port)) {
                observerPort = ob.getInt(ConfigTag_Port);
			}

			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(clientPort);

			// Start deamon for writing log to BD
			Common.GetSingletonExecSvc().execute(adaptor);

			// Start WS server for observers
			Common.GetSingletonExecSvc().execute(wsSvr);

			// Heartbeat deamon
			Common.GetSingletonExecSvc().execute(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							// prime number
							Thread.sleep(WsHeartbeat_Intvl);
						} catch (InterruptedException e) {
						}

						// Send heartbeats
						subs.sendHeartbeat(HeartbeatMsg);
					}
				}

			});

			info("LOGDB is listening on port: " + clientPort);

			while (true) {
				Socket client = ss.accept();

				// get client address
				InetSocketAddress addr = (InetSocketAddress)client.getRemoteSocketAddress();
				String remoteIP = addr.getAddress().getHostAddress();

				InputStream is0 = null;
				try {
					is0 = Constants.class.getResourceAsStream("ip.json");
				} catch (Exception ex) {
					Common.PrintException(ex);
				}

				// validate IP
				if (!Common.VerifyIP(remoteIP, is0)) {
					info("Invalid remote IP: " + remoteIP);
					client.close();
					continue;
				}

				// set OOB
				client.setOOBInline(false);

				// add logging service instance
				rwLock.writeLock().lock();
				servers.add(new LoggingServer(client, adaptor, subs));
				rwLock.writeLock().unlock();

				// remove dead service
				Set<LoggingServer> tmp = new HashSet<LoggingServer>();
				rwLock.readLock().lock();
				for (LoggingServer s : servers) {
					if (!s.IsConnected()) {
						tmp.add(s);
					}
				}
				rwLock.readLock().unlock();
				rwLock.writeLock().lock();
				for (LoggingServer s : tmp) {
					servers.remove(s);
				}
				rwLock.writeLock().unlock();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}

	@Override
	public void OnStream(byte[] Data) {
		try {
			String text = new String(Data, 0, Data.length, DefaultCharset);
			SingleLog log = LoggingDbAdaptor.createLog(text);

			// Write log to DB
			adaptor.insertLog(log);

			// Send log to observers
			subscriber.distributeLog(log);
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}

	@Override
	public void OnDisconnect() {
		InetSocketAddress addr = (InetSocketAddress) this.GetSocketAddress();
		info("Client disconnected, " + addr.getHostString() + ":" + addr.getPort());
	}

	@Override
	public void OnConnect() {
		InetSocketAddress addr = (InetSocketAddress) this.GetSocketAddress();
		info("Client connected, " + addr.getHostString() + ":" + addr.getPort());

		Thread.currentThread().setName("Client session (" + addr.getHostString() + ":" + addr.getPort() + ")");
	}
}
