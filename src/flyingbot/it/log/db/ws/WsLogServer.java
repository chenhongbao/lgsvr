package flyingbot.it.log.db.ws;

import flyingbot.it.log.db.LoggingServer;
import flyingbot.it.log.db.resources.Constants;
import flyingbot.it.util.Common;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.json.JSONObject;

import java.net.InetSocketAddress;

import static flyingbot.it.log.db.resources.Constants.*;

public class WsLogServer implements Runnable {
    /**
     * For short request like WebSocket, Old IO is more efficient.
     */
    private final EventLoopGroup group = new NioEventLoopGroup();
    int port;
    /**
     * Subscribers
     */
    LogSubscriber subscriber;
    /**
     * Context
     */
    WsLogServerContext serverCtx;
    /**
     * Server listening channel
     */
    private Channel channel;

    WsLogServer(int port, LogSubscriber sub) {
        this.port = port;
        this.subscriber = sub;
        this.serverCtx = new WsLogServerContext();
    }

    protected static int getListenPort() {
        // Load JSON as stream
        JSONObject o = Common.LoadJSONObject(Constants.class.getResourceAsStream("ws_port.json"));
        if (!o.has(ConfigTag_Port)) {
            LoggingServer.warn("Reading listening port JSON failed, listen on DEFAULT port: " + ObserverListenPort);
            return ObserverListenPort;
        } else {
            return o.getInt(ConfigTag_Port);
        }
    }

    public static WsLogServer instance(int port, LogSubscriber subs) {
        return new WsLogServer(port, subs);
    }

    @Override
    public void run() {
        int port = getListenPort();
        LoggingServer.info("LOGDB WebSocket server is listening on port: " + port);

        // Create server instance
        final WsLogServer endpoint = WsLogServer.instance(port, subscriber);
        ChannelFuture future = endpoint.start(new InetSocketAddress(port));

        // Setup destroy
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                endpoint.destroy();
            }
        });

        // Close server channel
        future.channel().closeFuture().syncUninterruptibly();
    }

    public ChannelFuture start(InetSocketAddress address) {
        // Create server bootstrap
        ServerBootstrap bootstrap = new ServerBootstrap();

        // Setup bootstrap
        bootstrap.group(group);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChatServerInitializer(serverCtx, subscriber));

        // Bind address
        ChannelFuture future = bootstrap.bind(address);
        future.syncUninterruptibly();

        // Server listening channel, for client's connection
        channel = future.channel();

        // Return statement
        return future;
    }

    public void destroy() {
        if (channel != null) {
            channel.close();
        }

        // Close connection
        subscriber.closeAllChannels();
        group.shutdownGracefully();
    }

    public static class ChatServerInitializer extends ChannelInitializer<Channel> {

        /**
         * Server context
         */
        private WsLogServerContext svrCtx;

        /**
         * Subscribers
         */
        private LogSubscriber subscriber;

        public ChatServerInitializer(WsLogServerContext ctx, LogSubscriber subs) {
            this.svrCtx = ctx;
            this.subscriber = subs;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();

            // Add listener and handlers
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new ChunkedWriteHandler());
            pipeline.addLast(new HttpObjectAggregator(HTTP_MaxContentLength));
            pipeline.addLast(new HttpRequestHandler(svrCtx));

            // After update to WebSocket protocol, the handler will replace HttpRequestDecoder
            // with WebSocketFrameDecoder, and HttpResponseEncoder with WebSocketFrameEncoder, and
            // any other ChannelHandler that are not used any more.
            pipeline.addLast(new WebSocketServerProtocolHandler(wsURI, subProtocol));

            pipeline.addLast(new WsFrameHandler(subscriber));
        }
    }
}
