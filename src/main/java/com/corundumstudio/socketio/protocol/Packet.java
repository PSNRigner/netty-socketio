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

import com.corundumstudio.socketio.namespace.Namespace;
import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Packet implements Serializable {

    private static final long serialVersionUID = 4560159536486711426L;

    private PacketType type;
    private PacketType subType;
    private Long ackId;
    private String name;
    private String nsp = Namespace.DEFAULT_NAME;
    private Object data;

    private ByteBuf dataSource;
    private int attachmentsCount;
    private List<ByteBuf> attachments = Collections.emptyList();

    protected Packet() {
    }

    public Packet(final PacketType type) {
        super();
        this.type = type;
    }

    public PacketType getSubType() {
        return this.subType;
    }

    public void setSubType(final PacketType subType) {
        this.subType = subType;
    }

    public PacketType getType() {
        return this.type;
    }

    public void setData(final Object data) {
        this.data = data;
    }

    /**
     * Get packet data
     * 
     * @param <T> the type data
     * 
     * <pre>
     * @return <b>json object</b> for PacketType.JSON type
     * <b>message</b> for PacketType.MESSAGE type
     * </pre>
     */
    public <T> T getData() {
        return (T) this.data;
    }

    public void setNsp(final String endpoint) {
        this.nsp = endpoint;
    }

    public String getNsp() {
        return this.nsp;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public Long getAckId() {
        return this.ackId;
    }

    public void setAckId(final Long ackId) {
        this.ackId = ackId;
    }

    public boolean isAckRequested() {
        return this.getAckId() != null;
    }

    public void initAttachments(final int attachmentsCount) {
        this.attachmentsCount = attachmentsCount;
        this.attachments = new ArrayList<>(attachmentsCount);
    }

    public void addAttachment(final ByteBuf attachment) {
        if (this.attachments.size() < this.attachmentsCount) {
            this.attachments.add(attachment);
        }
    }
    public List<ByteBuf> getAttachments() {
        return this.attachments;
    }
    public boolean hasAttachments() {
        return this.attachmentsCount != 0;
    }
    public boolean isAttachmentsLoaded() {
        return this.attachments.size() == this.attachmentsCount;
    }

    public ByteBuf getDataSource() {
        return this.dataSource;
    }

    public void setDataSource(final ByteBuf dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String toString() {
        return "Packet [type=" + this.type + ", ackId=" + this.ackId + "]";
    }

}
