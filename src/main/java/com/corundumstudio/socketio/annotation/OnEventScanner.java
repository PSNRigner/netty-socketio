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
package com.corundumstudio.socketio.annotation;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.handler.SocketIOException;
import com.corundumstudio.socketio.namespace.Namespace;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class OnEventScanner implements AnnotationScanner {

    @Override
    public Class<? extends Annotation> getScanAnnotation() {
        return OnEvent.class;
    }

    @Override
    public void addListener(final Namespace namespace, final Object object, final Method method, final Annotation annot) {
        final OnEvent annotation = (OnEvent) annot;
        annotation.value();
        if (annotation.value().trim().length() == 0) {
            throw new IllegalArgumentException("OnEvent \"value\" parameter is required");
        }
        final int socketIOClientIndex = this.paramIndex(method, SocketIOClient.class);
        final int ackRequestIndex = this.paramIndex(method, AckRequest.class);
        final List<Integer> dataIndexes = this.dataIndexes(method);

        if (dataIndexes.size() > 1) {
            final List<Class<?>> classes = new ArrayList<>();
            for (final int index : dataIndexes) {
                final Class<?> param = method.getParameterTypes()[index];
                classes.add(param);
            }

            namespace.addMultiTypeEventListener(annotation.value(), (client, data, ackSender) -> {
                try {
                    final Object[] args = new Object[method.getParameterTypes().length];
                    if (socketIOClientIndex != -1) {
                        args[socketIOClientIndex] = client;
                    }
                    if (ackRequestIndex != -1) {
                        args[ackRequestIndex] = ackSender;
                    }
                    int i = 0;
                    for (final int index : dataIndexes) {
                        args[index] = data.get(i);
                        i++;
                    }
                    method.invoke(object, args);
                } catch (final InvocationTargetException e) {
                    throw new SocketIOException(e.getCause());
                } catch (final Exception e) {
                    throw new SocketIOException(e);
                }
            }, classes.toArray(new Class[classes.size()]));
        } else {
            Class objectType = Void.class;
            if (!dataIndexes.isEmpty()) {
                objectType = method.getParameterTypes()[dataIndexes.iterator().next()];
            }

            namespace.addEventListener(annotation.value(), objectType, (client, data, ackSender) -> {
                try {
                    final Object[] args = new Object[method.getParameterTypes().length];
                    if (socketIOClientIndex != -1) {
                        args[socketIOClientIndex] = client;
                    }
                    if (ackRequestIndex != -1) {
                        args[ackRequestIndex] = ackSender;
                    }
                    if (!dataIndexes.isEmpty()) {
                        final int dataIndex = dataIndexes.iterator().next();
                        args[dataIndex] = data;
                    }
                    method.invoke(object, args);
                } catch (final InvocationTargetException e) {
                    throw new SocketIOException(e.getCause());
                } catch (final Exception e) {
                    throw new SocketIOException(e);
                }
            });
        }
    }

    private List<Integer> dataIndexes(final Method method) {
        final List<Integer> result = new ArrayList<>();
        int index = 0;
        for (final Class<?> type : method.getParameterTypes()) {
            if (!type.equals(AckRequest.class) && !type.equals(SocketIOClient.class)) {
                result.add(index);
            }
            index++;
        }
        return result;
    }

    private int paramIndex(final Method method, final Class<?> clazz) {
        int index = 0;
        for (final Class<?> type : method.getParameterTypes()) {
            if (type.equals(clazz)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    @Override
    public void validate(final Method method, final Class<?> clazz) {
        int paramsCount = method.getParameterTypes().length;
        final int socketIOClientIndex = this.paramIndex(method, SocketIOClient.class);
        final int ackRequestIndex = this.paramIndex(method, AckRequest.class);
        final List<Integer> dataIndexes = this.dataIndexes(method);
        paramsCount -= dataIndexes.size();
        if (socketIOClientIndex != -1) {
            paramsCount--;
        }
        if (ackRequestIndex != -1) {
            paramsCount--;
        }
        if (paramsCount != 0) {
            throw new IllegalArgumentException("Wrong OnEvent listener signature: " + clazz + "." + method.getName());
        }
    }

}
