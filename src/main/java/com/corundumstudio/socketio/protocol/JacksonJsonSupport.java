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
package com.corundumstudio.socketio.protocol;

import com.corundumstudio.socketio.AckCallback;
import com.corundumstudio.socketio.MultiTypeAckCallback;
import com.corundumstudio.socketio.namespace.Namespace;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.ArrayType;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;


public class JacksonJsonSupport implements JsonSupport {

    private class AckArgsDeserializer extends StdDeserializer<AckArgs> {

        private static final long serialVersionUID = 7810461017389946707L;

        protected AckArgsDeserializer() {
            super(AckArgs.class);
        }

        @Override
        public AckArgs deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final List<Object> args = new ArrayList<>();
            final AckArgs result = new AckArgs(args);

            final ObjectMapper mapper = (ObjectMapper) jp.getCodec();
            final JsonNode root = mapper.readTree(jp);
            final AckCallback<?> callback = JacksonJsonSupport.this.currentAckClass.get();
            final Iterator<JsonNode> iter = root.iterator();
            int i = 0;
            while (iter.hasNext()) {
                final Object val;

                Class<?> clazz = callback.getResultClass();
                if (callback instanceof MultiTypeAckCallback) {
                    final MultiTypeAckCallback multiTypeAckCallback = (MultiTypeAckCallback) callback;
                    clazz = multiTypeAckCallback.getResultClasses()[i];
                }

                final JsonNode arg = iter.next();
                if (arg.isTextual() || arg.isBoolean()) {
                    clazz = Object.class;
                }

                val = mapper.treeToValue(arg, clazz);
                args.add(val);
                i++;
            }
            return result;
        }

    }

    public static class EventKey {

        private final String namespaceName;
        private final String eventName;

        public EventKey(final String namespaceName, final String eventName) {
            super();
            this.namespaceName = namespaceName;
            this.eventName = eventName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.eventName == null) ? 0 : this.eventName.hashCode());
            result = prime * result + ((this.namespaceName == null) ? 0 : this.namespaceName.hashCode());
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
            final EventKey other = (EventKey) obj;
            if (this.eventName == null) {
                if (other.eventName != null) {
                    return false;
                }
            } else if (!this.eventName.equals(other.eventName)) {
                return false;
            }
            if (this.namespaceName == null) {
                if (other.namespaceName != null) {
                    return false;
                }
            } else if (!this.namespaceName.equals(other.namespaceName)) {
                return false;
            }
            return true;
        }

    }

    private class EventDeserializer extends StdDeserializer<Event> {

        private static final long serialVersionUID = 8178797221017768689L;

        final Map<EventKey, List<Class<?>>> eventMapping = PlatformDependent.newConcurrentHashMap();


        protected EventDeserializer() {
            super(Event.class);
        }

        @Override
        public Event deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final ObjectMapper mapper = (ObjectMapper) jp.getCodec();
            final String eventName = jp.nextTextValue();

            EventKey ek = new EventKey(JacksonJsonSupport.this.namespaceClass.get(), eventName);
            if (!this.eventMapping.containsKey(ek)) {
                ek = new EventKey(Namespace.DEFAULT_NAME, eventName);
                if (!this.eventMapping.containsKey(ek)) {
                    return new Event(eventName, Collections.emptyList());
                }
            }

            final List<Object> eventArgs = new ArrayList<>();
            final Event event = new Event(eventName, eventArgs);
            final List<Class<?>> eventClasses = this.eventMapping.get(ek);
            int i = 0;
            while (true) {
                final JsonToken token = jp.nextToken();
                if (token == JsonToken.END_ARRAY) {
                    break;
                }
                if (i > eventClasses.size() - 1) {
//                    log.debug("Event {} has more args than declared in handler: {}", eventName, null);
                    break;
                }
                final Class<?> eventClass = eventClasses.get(i);
                final Object arg = mapper.readValue(jp, eventClass);
                eventArgs.add(arg);
                i++;
            }
            return event;
        }

    }

    public static class ByteArraySerializer extends StdSerializer<byte[]>
    {

        private static final long serialVersionUID = 3420082888596468148L;

        private final ThreadLocal<List<byte[]>> arrays = ThreadLocal.withInitial(() -> new ArrayList<>());

        public ByteArraySerializer() {
            super(byte[].class);
        }

        @Override
        public boolean isEmpty(final SerializerProvider provider, final byte[] value) {
            return (value == null) || (value.length == 0);
        }

        @Override
        public void serialize(final byte[] value, final JsonGenerator jgen, final SerializerProvider provider)
            throws IOException {
            final Map<String, Object> map = new HashMap<>();
            map.put("num", this.arrays.get().size());
            map.put("_placeholder", true);
            jgen.writeObject(map);
            this.arrays.get().add(value);
        }

        @Override
        public void serializeWithType(final byte[] value, final JsonGenerator jgen, final SerializerProvider provider,
                                      final TypeSerializer typeSer)
            throws IOException {
            this.serialize(value, jgen, provider);
        }

        @Override
        public JsonNode getSchema(final SerializerProvider provider, final Type typeHint)
        {
            final ObjectNode o = this.createSchemaNode("array", true);
            final ObjectNode itemSchema = this.createSchemaNode("string"); //binary values written as strings?
            return o.set("items", itemSchema);
        }

        @Override
        public void acceptJsonFormatVisitor(final JsonFormatVisitorWrapper visitor, final JavaType typeHint)
                throws JsonMappingException
        {
            if (visitor != null) {
                final JsonArrayFormatVisitor v2 = visitor.expectArrayFormat(typeHint);
                if (v2 != null) {
                    v2.itemsFormat(JsonFormatTypes.STRING);
                }
            }
        }

        public List<byte[]> getArrays() {
            return this.arrays.get();
        }

        public void clear() {
            this.arrays.set(new ArrayList<>());
        }

    }


    private class ExBeanSerializerModifier extends BeanSerializerModifier {

        private final ByteArraySerializer serializer = new ByteArraySerializer();

        @Override
        public JsonSerializer<?> modifyArraySerializer(final SerializationConfig config, final ArrayType valueType,
                                                       final BeanDescription beanDesc, final JsonSerializer<?> serializer) {
            if (valueType.getRawClass().equals(byte[].class)) {
                return this.serializer;
            }

            return super.modifyArraySerializer(config, valueType, beanDesc, serializer);
        }

        public ByteArraySerializer getSerializer() {
            return this.serializer;
        }

    }

    protected final ExBeanSerializerModifier modifier = new ExBeanSerializerModifier();
    protected final ThreadLocal<String> namespaceClass = new ThreadLocal<>();
    protected final ThreadLocal<AckCallback<?>> currentAckClass = new ThreadLocal<>();
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final EventDeserializer eventDeserializer = new EventDeserializer();
    protected final AckArgsDeserializer ackArgsDeserializer = new AckArgsDeserializer();


    public JacksonJsonSupport() {
        this(new Module[] {});
    }

    public JacksonJsonSupport(final Module... modules) {
        if (modules != null && modules.length > 0) {
            this.objectMapper.registerModules(modules);
        }
        this.init(this.objectMapper);
    }

    protected void init(final ObjectMapper objectMapper) {
        final SimpleModule module = new SimpleModule();
        module.setSerializerModifier(this.modifier);
        module.addDeserializer(Event.class, this.eventDeserializer);
        module.addDeserializer(AckArgs.class, this.ackArgsDeserializer);
        objectMapper.registerModule(module);

        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    public void addEventMapping(final String namespaceName, final String eventName, final Class<?>... eventClass) {
        this.eventDeserializer.eventMapping.put(new EventKey(namespaceName, eventName), Arrays.asList(eventClass));
    }

    @Override
    public void removeEventMapping(final String namespaceName, final String eventName) {
        this.eventDeserializer.eventMapping.remove(new EventKey(namespaceName, eventName));
    }

    @Override
    public <T> T readValue(final String namespaceName, final ByteBufInputStream src, final Class<T> valueType) throws IOException {
        this.namespaceClass.set(namespaceName);
        return this.objectMapper.readValue(src, valueType);
    }

    @Override
    public AckArgs readAckArgs(final ByteBufInputStream src, final AckCallback<?> callback) throws IOException {
        this.currentAckClass.set(callback);
        return this.objectMapper.readValue(src, AckArgs.class);
    }

    @Override
    public void writeValue(final ByteBufOutputStream out, final Object value) throws IOException {
        this.modifier.getSerializer().clear();
        this.objectMapper.writeValue(out, value);
    }

    @Override
    public List<byte[]> getArrays() {
        return this.modifier.getSerializer().getArrays();
    }

}
