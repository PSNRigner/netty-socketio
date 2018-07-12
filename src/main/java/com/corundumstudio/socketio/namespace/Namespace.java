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
package com.corundumstudio.socketio.namespace;

import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.annotation.ScannerEngine;
import com.corundumstudio.socketio.listener.*;
import com.corundumstudio.socketio.protocol.JsonSupport;
import com.corundumstudio.socketio.protocol.Packet;
import com.corundumstudio.socketio.store.StoreFactory;
import com.corundumstudio.socketio.store.pubsub.JoinLeaveMessage;
import com.corundumstudio.socketio.store.pubsub.PubSubType;
import com.corundumstudio.socketio.transport.NamespaceClient;
import io.netty.util.internal.PlatformDependent;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Hub object for all clients in one namespace.
 * Namespace shares by different namespace-clients.
 *
 * @see com.corundumstudio.socketio.transport.NamespaceClient
 */
public class Namespace implements SocketIONamespace {

    public static final String DEFAULT_NAME = "";

    private final ScannerEngine engine = new ScannerEngine();
    private final ConcurrentMap<String, EventEntry<?>> eventListeners = PlatformDependent.newConcurrentHashMap();
    private final Queue<ConnectListener> connectListeners = new ConcurrentLinkedQueue<>();
    private final Queue<DisconnectListener> disconnectListeners = new ConcurrentLinkedQueue<>();
    private final Queue<PingListener> pingListeners = new ConcurrentLinkedQueue<>();

    private final Map<UUID, SocketIOClient> allClients = PlatformDependent.newConcurrentHashMap();
    private final ConcurrentMap<String, Set<UUID>> roomClients = PlatformDependent.newConcurrentHashMap();
    private final ConcurrentMap<UUID, Set<String>> clientRooms = PlatformDependent.newConcurrentHashMap();

    private final String name;
    private final AckMode ackMode;
    private final JsonSupport jsonSupport;
    private final StoreFactory storeFactory;
    private final ExceptionListener exceptionListener;

    public Namespace(final String name, final Configuration configuration) {
        super();
        this.name = name;
        this.jsonSupport = configuration.getJsonSupport();
        this.storeFactory = configuration.getStoreFactory();
        this.exceptionListener = configuration.getExceptionListener();
        this.ackMode = configuration.getAckMode();
    }

    public void addClient(final SocketIOClient client) {
        this.allClients.put(client.getSessionId(), client);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void addMultiTypeEventListener(final String eventName, final MultiTypeEventListener listener,
                                          final Class<?>... eventClass) {
        EventEntry entry = this.eventListeners.get(eventName);
        if (entry == null) {
            entry = new EventEntry();
            final EventEntry<?> oldEntry = this.eventListeners.putIfAbsent(eventName, entry);
            if (oldEntry != null) {
                entry = oldEntry;
            }
        }
        entry.addListener(listener);
        this.jsonSupport.addEventMapping(this.name, eventName, eventClass);
    }
    
    @Override
    public void removeAllListeners(final String eventName) {
        final EventEntry<?> entry = this.eventListeners.remove(eventName);
        if (entry != null) {
            this.jsonSupport.removeEventMapping(this.name, eventName);
        }
    }

    @Override
    public <T> void addEventListener(final String eventName, final Class<T> eventClass, final DataListener<T> listener) {
        EventEntry entry = this.eventListeners.get(eventName);
        if (entry == null) {
            entry = new EventEntry<T>();
            final EventEntry<?> oldEntry = this.eventListeners.putIfAbsent(eventName, entry);
            if (oldEntry != null) {
                entry = oldEntry;
            }
        }
        entry.addListener(listener);
        this.jsonSupport.addEventMapping(this.name, eventName, eventClass);
    }

    public void onEvent(final NamespaceClient client, final String eventName, final List<Object> args, final AckRequest ackRequest) {
        final EventEntry entry = this.eventListeners.get(eventName);
        if (entry == null) {
            return;
        }

        try {
            final Queue<DataListener> listeners = entry.getListeners();
            for (final DataListener dataListener : listeners) {
                final Object data = this.getEventData(args, dataListener);
                dataListener.onData(client, data, ackRequest);
            }
        } catch (final Exception e) {
            this.exceptionListener.onEventException(e, args, client);
            if (this.ackMode == AckMode.AUTO_SUCCESS_ONLY) {
                return;
            }
        }

        this.sendAck(ackRequest);
    }

    private void sendAck(final AckRequest ackRequest) {
        if (this.ackMode == AckMode.AUTO || this.ackMode == AckMode.AUTO_SUCCESS_ONLY) {
            // send ack response if it not executed
            // during {@link DataListener#onData} invocation
            ackRequest.sendAckData(Collections.emptyList());
        }
    }

    private Object getEventData(final List<Object> args, final DataListener<?> dataListener) {
        if (dataListener instanceof MultiTypeEventListener) {
            return new MultiTypeArgs(args);
        } else {
            if (!args.isEmpty()) {
                return args.get(0);
            }
        }
        return null;
    }

    @Override
    public void addDisconnectListener(final DisconnectListener listener) {
        this.disconnectListeners.add(listener);
    }

    public void onDisconnect(final SocketIOClient client) {
        final Set<String> joinedRooms = client.getAllRooms();
        this.allClients.remove(client.getSessionId());

        this.leave(this.getName(), client.getSessionId());
        this.storeFactory.pubSubStore().publish(PubSubType.LEAVE, new JoinLeaveMessage(client.getSessionId(), this.getName(), this.getName()));

        for (final String joinedRoom : joinedRooms) {
            this.leave(this.roomClients, joinedRoom, client.getSessionId());
        }
        this.clientRooms.remove(client.getSessionId());

        try {
            for (final DisconnectListener listener : this.disconnectListeners) {
                listener.onDisconnect(client);
            }
        } catch (final Exception e) {
            this.exceptionListener.onDisconnectException(e, client);
        }
    }

    @Override
    public void addConnectListener(final ConnectListener listener) {
        this.connectListeners.add(listener);
    }

    public void onConnect(final SocketIOClient client) {
        this.join(this.getName(), client.getSessionId());
        this.storeFactory.pubSubStore().publish(PubSubType.JOIN, new JoinLeaveMessage(client.getSessionId(), this.getName(), this.getName()));

        try {
            this.connectListeners.forEach(listener -> listener.onConnect(client));
        } catch (final Exception e) {
            this.exceptionListener.onConnectException(e, client);
        }
    }

    @Override
    public void addPingListener(final PingListener listener) {
        this.pingListeners.add(listener);
    }

    public void onPing(final SocketIOClient client) {
        try {
            this.pingListeners.forEach(listener -> listener.onPing(client));
        } catch (final Exception e) {
            this.exceptionListener.onPingException(e, client);
        }
    }

    @Override
    public BroadcastOperations getBroadcastOperations() {
        return new BroadcastOperations(this.allClients.values(), this.storeFactory);
    }

    @Override
    public BroadcastOperations getRoomOperations(final String room) {
        return new BroadcastOperations(this.getRoomClients(room), this.storeFactory);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final Namespace other = (Namespace) obj;
        if (this.name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!this.name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public void addListeners(final Object listeners) {
        this.addListeners(listeners, listeners.getClass());
    }

    @Override
    public void addListeners(final Object listeners, final Class<?> listenersClass) {
        this.engine.scan(this, listeners, listenersClass);
    }

    public void joinRoom(final String room, final UUID sessionId) {
        this.join(room, sessionId);
        this.storeFactory.pubSubStore().publish(PubSubType.JOIN, new JoinLeaveMessage(sessionId, room, this.getName()));
    }

    public void dispatch(final String room, final Packet packet) {
        final Iterable<SocketIOClient> clients = this.getRoomClients(room);

        for (final SocketIOClient socketIOClient : clients) {
            socketIOClient.send(packet);
        }
    }

    private <K, V> void join(final ConcurrentMap<K, Set<V>> map, final K key, final V value) {
        Set<V> clients = map.get(key);
        if (clients == null) {
            clients = Collections.newSetFromMap(PlatformDependent.<V, Boolean>newConcurrentHashMap());
            final Set<V> oldClients = map.putIfAbsent(key, clients);
            if (oldClients != null) {
                clients = oldClients;
            }
        }
        clients.add(value);
        // object may be changed due to other concurrent call
        if (clients != map.get(key)) {
            // re-join if queue has been replaced
            this.join(map, key, value);
        }
    }

    public void join(final String room, final UUID sessionId) {
        this.join(this.roomClients, room, sessionId);
        this.join(this.clientRooms, sessionId, room);
    }

    public void leaveRoom(final String room, final UUID sessionId) {
        this.leave(room, sessionId);
        this.storeFactory.pubSubStore().publish(PubSubType.LEAVE, new JoinLeaveMessage(sessionId, room, this.getName()));
    }

    private <K, V> void leave(final ConcurrentMap<K, Set<V>> map, final K room, final V sessionId) {
        final Set<V> clients = map.get(room);
        if (clients == null) {
            return;
        }
        clients.remove(sessionId);

        if (clients.isEmpty()) {
            map.remove(room, Collections.emptySet());
        }
    }

    public void leave(final String room, final UUID sessionId) {
        this.leave(this.roomClients, room, sessionId);
        this.leave(this.clientRooms, sessionId, room);
    }

    public Set<String> getRooms(final SocketIOClient client) {
        final Set<String> res = this.clientRooms.get(client.getSessionId());
        if (res == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(res);
    }

    public Set<String> getRooms() {
        return this.roomClients.keySet();
    }

    public Iterable<SocketIOClient> getRoomClients(final String room) {
        final Set<UUID> sessionIds = this.roomClients.get(room);

        if (sessionIds == null) {
            return Collections.emptyList();
        }

        final List<SocketIOClient> result = new ArrayList<>();
        for (final UUID sessionId : sessionIds) {
            final SocketIOClient client = this.allClients.get(sessionId);
            if(client != null) {
                result.add(client);
            }
        }
        return result;
    }

    @Override
    public Collection<SocketIOClient> getAllClients() {
        return Collections.unmodifiableCollection(this.allClients.values());
    }

    public JsonSupport getJsonSupport() {
        return this.jsonSupport;
    }

    @Override
    public SocketIOClient getClient(final UUID uuid) {
        return this.allClients.get(uuid);
    }

}
