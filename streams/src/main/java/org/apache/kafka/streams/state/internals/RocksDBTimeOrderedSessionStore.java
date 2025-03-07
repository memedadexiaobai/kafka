/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;

import java.util.Objects;

public class RocksDBTimeOrderedSessionStore
    extends WrappedStateStore<RocksDBTimeOrderedSessionSegmentedBytesStore, Object, Object>
    implements SessionStore<Bytes, byte[]> {

    private StateStoreContext stateStoreContext;

    RocksDBTimeOrderedSessionStore(final RocksDBTimeOrderedSessionSegmentedBytesStore store) {
        super(store);
        Objects.requireNonNull(store, "store is null");
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        wrapped().init(stateStoreContext, root);
        this.stateStoreContext = stateStoreContext;
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            getPosition(),
            stateStoreContext
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                  final long latestSessionEndTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchSessions(earliestSessionEndTime, latestSessionEndTime);
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessiontEndTime) {
        return wrapped().fetchSession(
            key,
            sessionStartTime,
            sessiontEndTime
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return backwardFindSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return findSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return backwardFindSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        wrapped().remove(sessionKey);
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        wrapped().put(sessionKey, aggregate);
    }
}
