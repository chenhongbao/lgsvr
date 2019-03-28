package flyingbot.it.log.db.ws;

import flyingbot.it.log.db.LoggingServer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import static flyingbot.it.log.db.resources.Constants.*;

/**
 * Observer sends request in the following format:
 * <p>
 * {
 * "sequence" : 1234,
 * "type" : "LogSubscription" | "LogUnsubscription",
 * "data" : [
 * {
 * "SubscriptionType" : "Like" | "Match",
 * "LoggerName" : "logger_name"
 * },
 * ...
 * ]
 * }
 */
public class WsFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    // Logger instance
    LogSubscriber subscriber;

    public WsFrameHandler(LogSubscriber sub) {
        this.subscriber = sub;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            // Will not use HTTP anymore.
            ctx.pipeline().remove(HttpRequestHandler.class);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
        try {
            String text = msg.text();
            JSONObject o = new JSONObject(text);

            // Parse un/subscription
            JSONArray arr = o.getJSONArray(DataTag);
            if (arr.length() < 1) {
                return;
            }

            String type = o.getString(TypeTag);

            // Try subscription
            for (int i = 0; i < arr.length(); ++i) {
                // Extract info
                JSONObject sub = arr.getJSONObject(i);
                String subTypeTag = sub.getString(SubscriptionTypeTag);
                String name = sub.getString(SubscriptionLoggerNameTag);

                // Decide subscription type
                LogSubscriber.SubscriptionType subType;
                if (subTypeTag.compareTo(SubscriptionType_Like) == 0) {
                    subType = LogSubscriber.SubscriptionType.LIKE;
                } else if (subTypeTag.compareTo(SubscriptionType_Match) == 0) {
                    subType = LogSubscriber.SubscriptionType.MATCH;
                } else {
                    LoggingServer.warn("Unknown subscription type: " + subTypeTag);
                    continue;
                }

                if (type.compareTo(RequestType_Sub) == 0) {
                    subscriber.subscribeLogger(ctx.channel(), name, subType);
                } else if (type.compareTo(RequestType_Uns) == 0) {
                    subscriber.unsubscribeLogger(ctx.channel(), name);
                } else {
                    LoggingServer.warn("JSON request type error, received " + type);
                }
            }
        } catch (Exception e) {
            LoggingServer.warn("Parse JSON failed, " + e.getMessage());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LoggingServer.warn("WebSocket error. " + ctx.channel() + ", " + cause.getMessage());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Don't need to un-subscribe instruments when Channel is closed.
        // The ChannelGroup will manage the closed channels.
        LoggingServer.info("Disconnect " + ctx.channel());
    }
}
