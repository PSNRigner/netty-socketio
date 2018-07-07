package com.corundumstudio.socketio;

import com.corundumstudio.socketio.handler.ClientHead;

@FunctionalInterface
public interface Disconnectable {

    void onDisconnect(ClientHead client);

}
