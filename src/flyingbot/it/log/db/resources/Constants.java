package flyingbot.it.log.db.resources;

public class Constants {
    // Default listening port for logging clients
    public final static int InsideListenPort = 9201;

    // Default listening port for logging observer
    public final static int ObserverListenPort = 8090;

    // Configuration tags
    public final static String ConfigTag_URL = "URL";
    public final static String ConfigTag_User = "Username";
    public final static String ConfigTag_Pwd = "Password";
    public final static String ConfigTag_Port = "Port";
    public final static String ConfigTag_IP = "IP";
    public final static String ConfigTag_Table = "Table";

    // Reconnect DB interval
    public final static long reconnectMillis = 1000 * 60 * 60;

    // Sleeping milliseconds between scanning queue
    public final static int QueueScanMillis = 1000;

    // do some inserts in a batch
    public final static long queryPerBatch = 500;

    // Default logging charset
    public final static String DefaultCharset = "UTF-8";

    // URI and subprotocol
    public final static String wsURI = "/subscribe";
    public final static String URIKey = "log.ws.uri";
    public final static String subProtocol = "flyingbot_logob_json_ws";

    // Request type
    public final static String RequestType_Sub = "LogSubscription";
    public final static String RequestType_Uns = "LogUnsubscription";

    // Subscription type
    public final static String SubscriptionTypeTag = "SubscriptionType";
    public final static String SubscriptionType_Like = "Like";
    public final static String SubscriptionType_Match = "Match";
    public final static String SubscriptionLoggerNameTag = "LoggerName";

    // JSON format tags
    public final static String DataTag = "data";
    public final static String TypeTag = "type";
    public final static String SequenceTag = "sequence";

    // Heartbeat message
    public final static String HeartbeatMsg = "{\"sequence\":0,\"type\":\"Heartbeat\",\"data\":[]}";

    // Heartbeat sending interval (ms)
    public final static int WsHeartbeat_Intvl = 17 * 1000;

    // HTTP max content
    public final static int HTTP_MaxContentLength = 64 * 1024;
}
