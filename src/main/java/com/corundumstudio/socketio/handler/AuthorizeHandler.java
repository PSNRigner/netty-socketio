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
package com.corundumstudio.socketio.handler;

import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.ack.AckManager;
import com.corundumstudio.socketio.messages.HttpErrorMessage;
import com.corundumstudio.socketio.namespace.Namespace;
import com.corundumstudio.socketio.namespace.NamespacesHub;
import com.corundumstudio.socketio.protocol.AuthPacket;
import com.corundumstudio.socketio.protocol.Packet;
import com.corundumstudio.socketio.protocol.PacketType;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.scheduler.SchedulerKey;
import com.corundumstudio.socketio.scheduler.SchedulerKey.Type;
import com.corundumstudio.socketio.store.StoreFactory;
import com.corundumstudio.socketio.store.pubsub.ConnectMessage;
import com.corundumstudio.socketio.store.pubsub.PubSubType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Sharable
public class AuthorizeHandler extends ChannelInboundHandlerAdapter implements Disconnectable {

    private static final Logger log = LoggerFactory.getLogger(AuthorizeHandler.class);

    private final CancelableScheduler disconnectScheduler;

    private final String connectPath;
    private final Configuration configuration;
    private final NamespacesHub namespacesHub;
    private final StoreFactory storeFactory;
    private final DisconnectableHub disconnectable;
    private final AckManager ackManager;
    private final ClientsBox clientsBox;

    public AuthorizeHandler(final String connectPath, final CancelableScheduler scheduler, final Configuration configuration, final NamespacesHub namespacesHub, final StoreFactory storeFactory,
                            final DisconnectableHub disconnectable, final AckManager ackManager, final ClientsBox clientsBox) {
        super();
        this.connectPath = connectPath;
        this.configuration = configuration;
        this.disconnectScheduler = scheduler;
        this.namespacesHub = namespacesHub;
        this.storeFactory = storeFactory;
        this.disconnectable = disconnectable;
        this.ackManager = ackManager;
        this.clientsBox = clientsBox;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        final SchedulerKey key = new SchedulerKey(Type.PING_TIMEOUT, ctx.channel());
        this.disconnectScheduler.schedule(key, () -> {
            ctx.channel().close();
            log.debug("Client with ip {} opened channel but doesn't send any data! Channel closed!", ctx.channel().remoteAddress());
        }, this.configuration.getFirstDataTimeout(), TimeUnit.MILLISECONDS);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        final SchedulerKey key = new SchedulerKey(Type.PING_TIMEOUT, ctx.channel());
        this.disconnectScheduler.cancel(key);

        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest req = (FullHttpRequest) msg;

            final Channel channel = ctx.channel();
            final QueryStringDecoder queryDecoder = new QueryStringDecoder(req.uri());

            if (!this.configuration.isAllowCustomRequests()
                    && !queryDecoder.path().startsWith(this.connectPath)) {
                final HttpResponse res = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
                channel.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
                req.release();
                return;
            }

            final List<String> sid = queryDecoder.parameters().get("sid");
            if (queryDecoder.path().equals(this.connectPath)
                    && sid == null) {
                final String origin = req.headers().get(HttpHeaderNames.ORIGIN);
                if (!this.authorize(ctx, channel, origin, queryDecoder.parameters(), req)) {
                    req.release();
                    return;
                }
                // forward message to polling or websocket handler to bind channel
            }
        }
        ctx.fireChannelRead(msg);
    }

    private boolean authorize(final ChannelHandlerContext ctx, final Channel channel, final String origin, final Map<String, List<String>> params, final FullHttpRequest req) {
        final Map<String, List<String>> headers = new HashMap<>(req.headers().names().size());
        for (final String name : req.headers().names()) {
            final List<String> values = req.headers().getAll(name);
            headers.put(name, values);
        }

        final HandshakeData data = new HandshakeData(req.headers(), params,
                                                (InetSocketAddress)channel.remoteAddress(),
                                                (InetSocketAddress)channel.localAddress(),
                                                    req.uri(), origin != null && !origin.equalsIgnoreCase("null"));

        boolean result = false;
        try {
            result = this.configuration.getAuthorizationListener().isAuthorized(data);
        } catch (final Exception e) {
            log.error("Authorization error", e);
        }

        if (!result) {
            final HttpResponse res = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
            channel.writeAndFlush(res)
                    .addListener(ChannelFutureListener.CLOSE);
            log.debug("Handshake unauthorized, query params: {} headers: {}", params, headers);
            return false;
        }

        final UUID sessionId = this.generateOrGetSessionIdFromRequest(req.headers());

        final List<String> transportValue = params.get("transport");
        if (transportValue == null) {
            log.error("Got no transports for request {}", req.uri());

            final HttpResponse res = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
            channel.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
            return false;
        }

        final Transport transport = Transport.byName(transportValue.get(0));
        if (!this.configuration.getTransports().contains(transport)) {
            final Map<String, Object> errorData = new HashMap<>();
            errorData.put("code", 0);
            errorData.put("message", "Transport unknown");

            channel.attr(EncoderHandler.ORIGIN).set(origin);
            channel.writeAndFlush(new HttpErrorMessage(errorData));
            return false;
        }

        final ClientHead client = new ClientHead(sessionId, this.ackManager, this.disconnectable, this.storeFactory, data, this.clientsBox, transport, this.disconnectScheduler, this.configuration);
        channel.attr(ClientHead.CLIENT).set(client);
        this.clientsBox.addClient(client);

        String[] transports = {};
        if (this.configuration.getTransports().contains(Transport.WEBSOCKET)) {
            transports = new String[] {"websocket"};
        }

        final AuthPacket authPacket = new AuthPacket(sessionId, transports, this.configuration.getPingInterval(),
                this.configuration.getPingTimeout());
        final Packet packet = new Packet(PacketType.OPEN);
        packet.setData(authPacket);
        client.send(packet);

        client.schedulePingTimeout();
        log.debug("Handshake authorized for sessionId: {}, query params: {} headers: {}", sessionId, params, headers);
        return true;
    }

    /**
        This method will either generate a new random sessionId or will retrieve the value stored
        in the "io" cookie.  Failures to parse will cause a logging warning to be generated and a
        random uuid to be generated instead (same as not passing a cookie in the first place).
    */
    private UUID generateOrGetSessionIdFromRequest(final HttpHeaders headers) {
        final List<String> values = headers.getAll("io");
        if (values.size() == 1) {
            try {
                return UUID.fromString(values.get(0));
            } catch (final IllegalArgumentException iaex) {
                log.warn("Malformed UUID received for session! io=" + values.get(0));
            }
        }

        for (final String cookieHeader : headers.getAll(HttpHeaderNames.COOKIE)) {
            final Set<Cookie> cookies = ServerCookieDecoder.LAX.decode(cookieHeader);

            for (final Cookie cookie : cookies) {
                if (cookie.name().equals("io")) {
                    try {
                        return UUID.fromString(cookie.value());
                    } catch (final IllegalArgumentException iaex) {
                        log.warn("Malformed UUID received for session! io=" + cookie.value());
                    }
                }
            }
        }
        
        return UUID.randomUUID();
    }

    public void connect(final UUID sessionId) {
        final SchedulerKey key = new SchedulerKey(Type.PING_TIMEOUT, sessionId);
        this.disconnectScheduler.cancel(key);
    }

    public void connect(final ClientHead client) {
        final Namespace ns = this.namespacesHub.get(Namespace.DEFAULT_NAME);

        if (!client.getNamespaces().contains(ns)) {
            final Packet packet = new Packet(PacketType.MESSAGE);
            packet.setSubType(PacketType.CONNECT);
            client.send(packet);

            this.configuration.getStoreFactory().pubSubStore().publish(PubSubType.CONNECT, new ConnectMessage(client.getSessionId()));

            final SocketIOClient nsClient = client.addNamespaceClient(ns);
            ns.onConnect(nsClient);
        }
    }

    @Override
    public void onDisconnect(final ClientHead client) {
        this.clientsBox.removeClient(client.getSessionId());
    }

}
