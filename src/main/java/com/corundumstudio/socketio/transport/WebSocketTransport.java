/**
 * Copyright 2012 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.corundumstudio.socketio.transport;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOChannelInitializer;
import com.corundumstudio.socketio.Transport;
import com.corundumstudio.socketio.handler.AuthorizeHandler;
import com.corundumstudio.socketio.handler.ClientHead;
import com.corundumstudio.socketio.handler.ClientsBox;
import com.corundumstudio.socketio.messages.PacketsMessage;
import com.corundumstudio.socketio.protocol.Packet;
import com.corundumstudio.socketio.protocol.PacketType;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.scheduler.SchedulerKey;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Sharable
public class WebSocketTransport extends ChannelInboundHandlerAdapter {

    public static final String NAME = "websocket";

    private static final Logger log = LoggerFactory.getLogger(WebSocketTransport.class);

    private final AuthorizeHandler authorizeHandler;
    private final CancelableScheduler scheduler;
    private final Configuration configuration;
    private final ClientsBox clientsBox;

    private final boolean isSsl;

    public WebSocketTransport(final boolean isSsl,
                              final AuthorizeHandler authorizeHandler, final Configuration configuration,
                              final CancelableScheduler scheduler, final ClientsBox clientsBox) {
        this.isSsl = isSsl;
        this.authorizeHandler = authorizeHandler;
        this.configuration = configuration;
        this.scheduler = scheduler;
        this.clientsBox = clientsBox;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        final Channel channel = ctx.channel();
        if (channel.isActive()) {
            ctx.close();
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof CloseWebSocketFrame) {
          ctx.channel().writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE);
        } else if (msg instanceof BinaryWebSocketFrame
                    || msg instanceof TextWebSocketFrame) {
            final ByteBufHolder frame = (ByteBufHolder) msg;
            final ClientHead client = this.clientsBox.get(ctx.channel());
            if (client == null) {
                log.debug("Client with was already disconnected. Channel closed!");
                ctx.channel().close();
                frame.release();
                return;
            }

            ctx.pipeline().fireChannelRead(new PacketsMessage(client, frame.content(), Transport.WEBSOCKET));
            frame.release();
        } else if (msg instanceof FullHttpRequest) {
            final FullHttpRequest req = (FullHttpRequest) msg;
            final QueryStringDecoder queryDecoder = new QueryStringDecoder(req.uri());
            final String path = queryDecoder.path();
            final List<String> transport = queryDecoder.parameters().get("transport");
            final List<String> sid = queryDecoder.parameters().get("sid");

            if (transport != null && NAME.equals(transport.get(0))) {
                try {
                    if (!this.configuration.getTransports().contains(Transport.WEBSOCKET)) {
                        log.debug("{} transport not supported by configuration.", Transport.WEBSOCKET);
                        ctx.channel().close();
                        return;
                    }
                    if (sid != null && sid.get(0) != null) {
                        final UUID sessionId = UUID.fromString(sid.get(0));
                        this.handshake(ctx, sessionId, path, req);
                    } else {
                        final ClientHead client = ctx.channel().attr(ClientHead.CLIENT).get();
                        // first connection
                        this.handshake(ctx, client.getSessionId(), path, req);
                    }
                } finally {
                    req.release();
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        final ClientHead client = this.clientsBox.get(ctx.channel());
        if (client != null && client.isTransportChannel(ctx.channel(), Transport.WEBSOCKET)) {
            ctx.flush();
        } else {
            super.channelReadComplete(ctx);
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        final  Channel channel = ctx.channel();
        final ClientHead client = this.clientsBox.get(channel);
        final Packet packet = new Packet(PacketType.MESSAGE);
        packet.setSubType(PacketType.DISCONNECT);
        if (client != null && client.isTransportChannel(ctx.channel(), Transport.WEBSOCKET)) {
            log.debug("channel inactive {}", client.getSessionId());
            client.onChannelDisconnect();
        }
        super.channelInactive(ctx);
        if (client != null) {
            client.send(packet);
        }
        channel.close();
        ctx.close();
    }

    private void handshake(final ChannelHandlerContext ctx, final UUID sessionId, final String path, final FullHttpRequest req) {
        final Channel channel = ctx.channel();

        final WebSocketServerHandshakerFactory factory =
                new WebSocketServerHandshakerFactory(this.getWebSocketLocation(req), null, true, this.configuration.getMaxFramePayloadLength());
        final WebSocketServerHandshaker handshaker = factory.newHandshaker(req);
        if (handshaker != null) {
            final ChannelFuture f = handshaker.handshake(channel, req);
            f.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.error("Can't handshake " + sessionId, future.cause());
                    return;
                }

                channel.pipeline().addBefore(SocketIOChannelInitializer.WEB_SOCKET_TRANSPORT, SocketIOChannelInitializer.WEB_SOCKET_AGGREGATOR,
                        new WebSocketFrameAggregator(this.configuration.getMaxFramePayloadLength()));
                this.connectClient(channel, sessionId);
            });
        } else {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        }
    }

    private void connectClient(final Channel channel, final UUID sessionId) {
        final ClientHead client = this.clientsBox.get(sessionId);
        if (client == null) {
            log.warn("Unauthorized client with sessionId: {} with ip: {}. Channel closed!",
                        sessionId, channel.remoteAddress());
            channel.close();
            return;
        }

        client.bindChannel(channel, Transport.WEBSOCKET);

        this.authorizeHandler.connect(client);

        if (client.getCurrentTransport() == Transport.POLLING) {
            final SchedulerKey key = new SchedulerKey(SchedulerKey.Type.UPGRADE_TIMEOUT, sessionId);
            this.scheduler.schedule(key, () -> {
                final ClientHead clientHead = this.clientsBox.get(sessionId);
                if (clientHead != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("client did not complete upgrade - closing transport");
                    }
                    clientHead.onChannelDisconnect();
                }
            }, this.configuration.getUpgradeTimeout(), TimeUnit.MILLISECONDS);
        }

        log.debug("сlient {} handshake completed", sessionId);
    }

    private String getWebSocketLocation(final HttpRequest req) {
        String protocol = "ws://";
        if (this.isSsl) {
            protocol = "wss://";
        }
        return protocol + req.headers().get(HttpHeaderNames.HOST) + req.uri();
    }

}
