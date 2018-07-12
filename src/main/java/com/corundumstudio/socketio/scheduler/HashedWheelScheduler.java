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
package com.corundumstudio.socketio.scheduler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.internal.PlatformDependent;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class HashedWheelScheduler implements CancelableScheduler {

    private final Map<SchedulerKey, Timeout> scheduledFutures = PlatformDependent.newConcurrentHashMap();
    private final HashedWheelTimer executorService;
    
    public HashedWheelScheduler() {
        this.executorService = new HashedWheelTimer();
    }

    public HashedWheelScheduler(final ThreadFactory threadFactory) {
        this.executorService = new HashedWheelTimer(threadFactory);
    }

    private volatile ChannelHandlerContext ctx;

    @Override
    public void update(final ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void cancel(final SchedulerKey key) {
        final Timeout timeout = this.scheduledFutures.remove(key);
        if (timeout != null) {
            timeout.cancel();
        }
    }

    @Override
    public void schedule(final Runnable runnable, final long delay, final TimeUnit unit) {
        this.executorService.newTimeout(timeout -> runnable.run(), delay, unit);
    }

    @Override
    public void scheduleCallback(final SchedulerKey key, final Runnable runnable, final long delay, final TimeUnit unit) {
        final Timeout timeout = this.executorService.newTimeout(timeout1 -> this.ctx.executor().execute(() -> {
            try {
                runnable.run();
            } finally {
                this.scheduledFutures.remove(key);
            }
        }), delay, unit);

        if (!timeout.isExpired()) {
            this.scheduledFutures.put(key, timeout);
        }
    }

    @Override
    public void schedule(final SchedulerKey key, final Runnable runnable, final long delay, final TimeUnit unit) {
        final Timeout timeout = this.executorService.newTimeout(timeout1 -> {
            try {
                runnable.run();
            } finally {
                this.scheduledFutures.remove(key);
            }
        }, delay, unit);

        if (!timeout.isExpired()) {
            this.scheduledFutures.put(key, timeout);
        }
    }

    @Override
    public void shutdown() {
        this.executorService.stop();
    }

}
