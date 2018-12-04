package dmkp.logging.svr;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONObject;

import dmkp.common.net.SocketDuplex;
import dmkp.common.util.Common;
import dmkp.common.util.Result;
import dmkp.logging.svr.db.LoggingDbAdaptor;
import dmkp.logging.svr.db.LoggingDbAdaptor.SingleLog;

public class LoggingServer extends SocketDuplex {

	
	/*���ݿ�����߳�*/
	LoggingDbAdaptor _Adaptor;

	public static int DEFAULT_PORT = 9201;

	public LoggingServer(Socket Sock, LoggingDbAdaptor Adaptor) {
		super(Sock);
		_Adaptor = Adaptor;
	}
	
	@Override
	public void OnConnect() {
		InetSocketAddress addr = (InetSocketAddress)this.GetSocketAddress();
		_LogSelf("Client connected, " + addr.getHostString() + ":" + addr.getPort(), _Adaptor);
		
		// �����߳�����
		Thread.currentThread().setName("Client session (" + addr.getHostString() + ":" + addr.getPort() + ")");
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
	
	private static void _LogSelf(String msg, LoggingDbAdaptor adaptor) {
		
		SingleLog log = new SingleLog();
		log.TimeStamp = Common.GetTimestamp();
		log.Level = "INFO";
		log.LineNumber = 0;
		log.LoggerName = "LoggerService";
		log.Message = msg;
		log.Millis = System.currentTimeMillis();
		log.SourceClassName = "dmkp.logging.svr.LoggingServer";
		log.SourceMethodName = "OnDisconnect";
		adaptor.InsertLog(log);
	}

	public static void main(String[] args) {
		int port = DEFAULT_PORT;
		LoggingDbAdaptor adaptor = null;
		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();
			// IP����
			InputStream is0 = Class.forName(traces[1].getClassName()).getResource("ip.json").openStream();
			// �����˿�
			InputStream is1 = Class.forName(traces[1].getClassName()).getResource("port.json").openStream();
			JSONObject ob = Common.LoadJSONObject(is1);
			if (ob.has("Port")) {
				port = ob.getInt("Port");
			}
			/*����socket��Զ�����˳�*/
			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(port);
			
			/*ִ�����ݿ�����߳�*/
			adaptor = LoggingDbAdaptor.CreateSingleton();
			Common.GetSingletonExecSvc().execute(adaptor);
			
			/*�����˿�*/
			System.out.println("��־�������������ڶ˿�" + port + "������");
			while (true) {
				/*��������*/
				Socket client = ss.accept();
				
				// �ж�Զ��IP��ַ�Ƿ���������
				InetSocketAddress addr = (InetSocketAddress)client.getRemoteSocketAddress();
				String remoteIP = addr.getAddress().getHostAddress();
				if (!Common.VerifyIP(remoteIP, is0)) {
					_LogSelf("�ܽ����ӣ����� " + remoteIP, adaptor);
					continue;
				}
				
				// ���ô����ֽڲ�����
				client.setOOBInline(false);
				
				/*ͬ��*/
				_lock.writeLock().lock();
				servers.add(new LoggingServer(client, adaptor));
				_lock.writeLock().unlock();
				
				/*������������Ƿ�Ϸ������Ϸ���ɾ��*/
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
