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
package com.corundumstudio.socketio.misc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompositeIterable<T> implements Iterable<T>, Iterator<T> {

    private List<Iterable<T>> iterablesList;
    private Iterable<T>[] iterables;

    private Iterator<Iterator<T>> listIterator;
    private Iterator<T> currentIterator;

    public CompositeIterable(final List<Iterable<T>> iterables) {
        this.iterablesList = iterables;
    }

    public CompositeIterable(final Iterable<T>... iterables) {
        this.iterables = iterables;
    }

    public CompositeIterable(final CompositeIterable<T> iterable) {
        this.iterables = iterable.iterables;
        this.iterablesList = iterable.iterablesList;
    }

    @Override
    public Iterator<T> iterator() {
        final List<Iterator<T>> iterators = new ArrayList<>();
        if (this.iterables != null) {
            for (final Iterable<T> iterable : this.iterables) {
                iterators.add(iterable.iterator());
            }
        } else {
            for (final Iterable<T> iterable : this.iterablesList) {
                iterators.add(iterable.iterator());
            }
        }
        this.listIterator = iterators.iterator();
        this.currentIterator = null;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (this.currentIterator == null || !this.currentIterator.hasNext()) {
            while (this.listIterator.hasNext()) {
                final Iterator<T> iterator = this.listIterator.next();
                if (iterator.hasNext()) {
                    this.currentIterator = iterator;
                    return true;
                }
            }
            return false;
        }
        return this.currentIterator.hasNext();
    }

    @Override
    public T next() {
        this.hasNext();
        return this.currentIterator.next();
    }

    @Override
    public void remove() {
        this.currentIterator.remove();
    }

}
