package flyingbot.it.log.db.ws;

import flyingbot.it.data.log.SingleLog;
import flyingbot.it.log.db.LoggingServer;
import flyingbot.it.util.Common;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import static flyingbot.it.log.db.resources.Constants.CacheLogMaxnum;
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

    /**
     * Logs to be sent to observers
     */
    ConcurrentLinkedQueue<SingleLog> logQueue;

    /**
     * Cache log
     */
    LogCache cache;

    LogSubscriber() {
        matchSubscription = new ConcurrentHashMap<>();
        likeSubscription = new ConcurrentHashMap<>();
        logQueue = new ConcurrentLinkedQueue<>();
        cache = new LogCache();

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

    public void distributeLog(SingleLog log) {
        logQueue.add(log);
        cache.cacheLog(log);
    }

    public static LogSubscriber singleton() {
        return singletonObj;
    }

    public void subscribeLogger(Channel ch, String logger, SubscriptionType type) {
        switch (type) {
            case MATCH:
                // Send old logs
                List<SingleLog> l = cache.queryLog(logger);
                if (l != null && l.size() > 0) {
                    for (SingleLog sl : l) {
                        try {
                            ch.writeAndFlush(sl.ToJSON().toString(-1));
                        } catch (Exception ex) {
                            LoggingServer.warn("Send log failed, " + ex.getMessage());
                        }
                    }
                }

                // Add to channel group
                addChannel(ch, logger, matchSubscription);
                break;
            case LIKE:
                // Send old logs
                Set<SingleLog> ret = new TreeSet<>();
                for (String k : cache.keys()) {
                    if (k.indexOf(logger) == -1) {
                        continue;
                    }

                    l = cache.queryLog(k);
                    if (l == null || l.size() < 1) {
                        continue;
                    }

                    // Insert log into a sorted set
                    for (SingleLog sl : l) {
                        ret.add(sl);
                    }
                }

                for (SingleLog sl : ret) {
                    try {
                        ch.writeAndFlush(sl.ToJSON().toString(-1));
                    } catch (Exception ex) {
                        LoggingServer.warn("Send log failed, " + ex.getMessage());
                    }
                }

                // Add to channel group
                addChannel(ch, logger, likeSubscription);
                break;
            default:
                break;
        }
    }

    void broadcastLog(SingleLog log) {
        if (matchSubscription.containsKey(log.LoggerName)) {
            try {
                matchSubscription.get(log.LoggerName).writeAndFlush(log.ToJSON().toString(-1));
            } catch (Exception ex) {
                LoggingServer.warn("Send log(" + log.LoggerName + ") to observer failed, " + ex.getMessage());
            }
        }

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
    }

    public boolean unsubscribeLogger(Channel ch, String logger) {
        return removeChannel(ch, logger, matchSubscription)
                || removeChannel(ch, logger, likeSubscription);
    }

    public void sendHeartbeat(String payload) {
        sendHeartbeatGroup(payload, matchSubscription);
        sendHeartbeatGroup(payload, likeSubscription);
    }

    void sendHeartbeatGroup(String payload, ConcurrentHashMap<String, ChannelGroup> group) {
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
    }

    public void closeAllChannels() {
        for (ChannelGroup g : matchSubscription.values()) {
            try {
                g.close();
            } catch (Exception ex) {
                LoggingServer.warn("Close channels failed, " + ex.getMessage());
            }
        }

        for (ChannelGroup g : likeSubscription.values()) {
            try {
                g.close();
            } catch (Exception ex) {
                LoggingServer.warn("Close channels failed, " + ex.getMessage());
            }
        }
    }

    void addChannel(Channel ch, String logger, ConcurrentHashMap<String, ChannelGroup> group) {
        if (!group.containsKey(logger)) {
            group.put(logger, new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE));
        }

        group.get(logger).add(ch);
    }

    boolean removeChannel(Channel ch, String logger, ConcurrentHashMap<String, ChannelGroup> group) {
        boolean ret = false;

        if (group.containsKey(logger)) {
            ret = group.get(logger).remove(ch);
        }

        return ret;
    }

    class LogCache {
        /**
         * Log cache
         */
        ConcurrentHashMap<String, ConcurrentLinkedQueue<SingleLog>> cache;

        public LogCache() {
            cache = new ConcurrentHashMap<>();
        }

        public Set<String> keys() {
            return cache.keySet();
        }

        public void cacheLog(SingleLog log) {
            if (!cache.containsKey(log.LoggerName)) {
                cache.put(log.LoggerName, new ConcurrentLinkedQueue<>());
            }

            // limit the size
            ConcurrentLinkedQueue<SingleLog> rec = cache.get(log.LoggerName);
            while (rec.size() > CacheLogMaxnum) {
                rec.poll();
            }

            // add to end
            rec.add(log);
        }

        public List<SingleLog> queryLog(String logger) {
            if (!cache.containsKey(logger)) {
                return null;
            }

            List<SingleLog> ret = new ArrayList<>();
            ret.addAll(cache.get(logger));
            return ret;
        }
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
