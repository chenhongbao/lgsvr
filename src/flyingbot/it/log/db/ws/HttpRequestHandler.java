package flyingbot.it.log.db.ws;

import flyingbot.it.log.db.LoggingServer;
import flyingbot.it.util.Common;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

import static flyingbot.it.log.db.resources.Constants.URIKey;
import static flyingbot.it.log.db.resources.Constants.wsURI;

public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    // Server context
    WsLogServerContext svrCtx;

    public HttpRequestHandler(WsLogServerContext ctx) {
        this.svrCtx = ctx;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        // Process websocket prefix
        String reqUri = request.uri();
        LoggingServer.info("Client connected, " + ctx.channel() + ", URI: " + reqUri);

        // Check if client requests the right resource
        if (reqUri.startsWith(wsURI)) {
            // Set path parameter
            String param = reqUri.substring(wsURI.length());
            svrCtx.channelParameter(ctx.channel(), URIKey, param);

            // Reset URI for WebSocketProtocolHandler, or it will block the request
            request.setUri(wsURI);

            // Fire next handler
            ctx.fireChannelRead(request.retain());
        } else {
            LoggingServer.warn("Client requests wrong resource, " + ctx.channel() + ", URI: " + reqUri);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // Log error
        LoggingServer.warn("Client request error, " + cause.getMessage());
        Common.PrintException(cause);

        // Close connection
        ctx.close();
    }
}
