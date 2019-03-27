package flyingbot.it.log.db.ws;

import flyingbot.it.data.log.SingleLog;
import flyingbot.it.log.db.LoggingServer;
import flyingbot.it.util.Common;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import static flyingbot.it.log.db.resources.Constants.QueueScanMillis;

public class LogSubscriber {
    /**
     * Singleton
     */
    static LogSubscriber singletonObj;
    static ReentrantLock sLock;

    static {
        singletonObj = new LogSubscriber();
        sLock = new ReentrantLock();
    }

    /**
     * Subscription channels
     */
    ConcurrentHashMap<String, ChannelGroup> matchSubscription, likeSubscription;
    ReentrantLock mwrLock, lwrLock;
    /**
     * Logs to be sent to observers
     */
    ConcurrentLinkedQueue<SingleLog> logQueue;

    LogSubscriber() {
        matchSubscription = new ConcurrentHashMap<>();
        likeSubscription = new ConcurrentHashMap<>();
        logQueue = new ConcurrentLinkedQueue<>();
        mwrLock = new ReentrantLock();
        lwrLock = new ReentrantLock();

        /**
         * Broadcast deamon
         */
        Common.GetSingletonExecSvc().execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        while (logQueue.size() > 0) {
                            SingleLog l = logQueue.poll();
                            if (l == null) {
                                continue;
                            }

                            broadcastLog(l);
                        }

                        Thread.sleep(QueueScanMillis);
                    } catch (Exception ex) {
                        LoggingServer.warn(ex.getMessage());
                    }
                }
            }
        });
    }

    public static LogSubscriber singleton() {
        return singletonObj;
    }

    public void distributeLog(SingleLog log) {
        logQueue.add(log);
    }

    void broadcastLog(SingleLog log) {
        mwrLock.lock();

        if (matchSubscription.containsKey(log.LoggerName)) {
            try {
                matchSubscription.get(log.LoggerName).writeAndFlush(log.ToJSON().toString(-1));
            } catch (Exception ex) {
                LoggingServer.warn("Send log(" + log.LoggerName + ") to observer failed, " + ex.getMessage());
            }
        }

        mwrLock.unlock();

        lwrLock.lock();

        for (String k : likeSubscription.keySet()) {
            if (log.LoggerName.indexOf(k) == -1) {
                continue;
            }

            try {
                likeSubscription.get(k).writeAndFlush(log.ToJSON().toString(-1));
            } catch (Exception ex) {
                LoggingServer.warn("Send log(" + log.LoggerName + ") to observer failed, " + ex.getMessage());
            }
        }

        lwrLock.unlock();
    }

    public void subscribeLogger(Channel ch, String logger, SubscriptionType type) {
        switch (type) {
            case MATCH:
                addChannel(ch, logger, mwrLock, matchSubscription);
                break;
            case LIKE:
                addChannel(ch, logger, lwrLock, likeSubscription);
                break;
            default:
                break;
        }
    }

    public boolean unsubscribeLogger(Channel ch, String logger) {
        return removeChannel(ch, logger, mwrLock, matchSubscription)
                || removeChannel(ch, logger, lwrLock, likeSubscription);
    }

    public void sendHeartbeat(String payload) {
        sendHeartbeatGroup(payload, mwrLock, matchSubscription);
        sendHeartbeatGroup(payload, lwrLock, likeSubscription);
    }

    public void closeAllChannels() {
        mwrLock.lock();

        for (ChannelGroup g : matchSubscription.values()) {
            try {
                g.close();
            } catch (Exception ex) {
                LoggingServer.warn("Close channels failed, " + ex.getMessage());
            }
        }

        mwrLock.unlock();

        lwrLock.lock();

        for (ChannelGroup g : likeSubscription.values()) {
            try {
                g.close();
            } catch (Exception ex) {
                LoggingServer.warn("Close channels failed, " + ex.getMessage());
            }
        }

        lwrLock.unlock();
    }

    void sendHeartbeatGroup(String payload, ReentrantLock lock, ConcurrentHashMap<String, ChannelGroup> group) {
        lock.lock();

        for (ChannelGroup g : group.values()) {
            if (g == null) {
                continue;
            }

            try {
                g.writeAndFlush(payload);
            } catch (Exception ex) {
                LoggingServer.warn(ex.getMessage());
            }
        }

        lock.unlock();
    }

    void addChannel(Channel ch, String logger, ReentrantLock lock, ConcurrentHashMap<String, ChannelGroup> group) {
        lock.lock();

        if (!group.containsKey(logger)) {
            group.put(logger, new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE));
        }

        group.get(logger).add(ch);

        lock.unlock();
    }

    boolean removeChannel(Channel ch, String logger, ReentrantLock lock, ConcurrentHashMap<String, ChannelGroup> group) {
        boolean ret = false;
        lock.lock();

        if (group.containsKey(logger)) {
            ret = group.get(logger).remove(ch);
        }

        lock.unlock();
        return ret;
    }

    /**
     * Define the match rule for the subscribed logger name
     */
    public enum SubscriptionType {
        // Matches the entire logger name
        MATCH,
        // Matches part of the logger name
        LIKE
    }
}
