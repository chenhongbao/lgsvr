package flyingbot.it.log.db.ws;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WsLogServerContext {

    /**
     * Channel parameter bundles
     */
    protected HashMap<Channel, ChannelParameterBundle> channelParams;
    protected ReentrantReadWriteLock cpLock;

    public WsLogServerContext() {
        // Channel parameters
        cpLock = new ReentrantReadWriteLock();
        channelParams = new HashMap<>();
    }

    /**
     * Set mapped object for the specific Channel.
     *
     * @param c     Channel
     * @param key   key
     * @param value Value
     */
    public void channelParameter(Channel c, String key, String value) {
        cpLock.writeLock().lock();

        if (!channelParams.containsKey(c)) {
            channelParams.put(c, new ChannelParameterBundle());
        }

        // Add parameter
        channelParams.get(c).parameter(key, value);

        cpLock.writeLock().unlock();
    }

    /**
     * Get the mapped object for the specific {@link Channel}.
     *
     * @param c Channel having the object.
     * @return object mapped for the channel, or null if no object found.
     */
    public String channelParameter(Channel c, String key) {
        String ret = null;
        cpLock.readLock().lock();

        if (channelParams.containsKey(c)) {
            ret = channelParams.get(c).parameter(key);
        }

        cpLock.readLock().unlock();
        return ret;
    }

    public static class ChannelParameterBundle {
        /**
         * Hash map for parameters
         */
        HashMap<String, String> params;

        public ChannelParameterBundle() {
            params = new HashMap<String, String>();
        }

        public void parameter(String key, String value) {
            params.put(key, value);
        }

        public String parameter(String key) {
            return params.get(key);
        }

        public String removeParameter(String key) {
            return params.remove(key);
        }

        public Map<String, String> parameters() {
            return params;
        }
    }
}
