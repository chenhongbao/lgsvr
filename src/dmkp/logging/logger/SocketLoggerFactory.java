package dmkp.logging.logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import dmkp.logging.logger.handler.JSONSocketHandler;

public class SocketLoggerFactory {
	
	static ReentrantReadWriteLock _lock = null;
	static Logger _log = null;
	
	static {
		_lock = new ReentrantReadWriteLock();
	}

	/**
	 * 获得网络日志对象实例。
	 * @param Name 日志名称
	 * @param IP 日志服务器IP地址
	 * @param Port 日志服务器端口
	 * @return 新的日志对象实例
	 */
	public static Logger GetInstance(String Name, String IP, int Port) {
		String name = Name == null || Name.length() < 1 ? SocketLoggerFactory.class.getCanonicalName() : Name;
		Logger tmp = Logger.getLogger(name);
		tmp.addHandler(new JSONSocketHandler(IP, Port));
		return tmp;
	}
	
	/**
	 * 获得日志对象单件。如果本次调用为首次获取该单件，则参数生效。否则参数不生效。
	 * @param Name 日志名称
	 * @param IP 日志服务器IP地址
	 * @param Port 日志服务器端口
	 * @return 新的日志对象单件
	 */
	public static Logger GetSingleton(String Name, String IP, int Port) {
		boolean b = false;
		_lock.readLock().lock();;
		b = _log == null;
		_lock.readLock().unlock();
		if (b) {
			_lock.writeLock().lock();
			if (_log == null) {
				_log = GetInstance(Name, IP, Port);
			}
			_lock.writeLock().unlock();
		}
		return _log;
	}
}
