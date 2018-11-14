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
	 * ���������־����ʵ����
	 * @param Name ��־����
	 * @param IP ��־������IP��ַ
	 * @param Port ��־�������˿�
	 * @return �µ���־����ʵ��
	 */
	public static Logger GetInstance(String Name, String IP, int Port) {
		String name = Name == null || Name.length() < 1 ? SocketLoggerFactory.class.getCanonicalName() : Name;
		Logger tmp = Logger.getLogger(name);
		tmp.addHandler(new JSONSocketHandler(IP, Port));
		return tmp;
	}
	
	/**
	 * �����־���󵥼���������ε���Ϊ�״λ�ȡ�õ������������Ч�������������Ч��
	 * @param Name ��־����
	 * @param IP ��־������IP��ַ
	 * @param Port ��־�������˿�
	 * @return �µ���־���󵥼�
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
