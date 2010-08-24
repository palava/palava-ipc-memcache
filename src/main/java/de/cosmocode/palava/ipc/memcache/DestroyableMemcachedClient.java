/**
 * Copyright 2010 CosmoCode GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.cosmocode.palava.ipc.memcache;

import de.cosmocode.palava.scope.Destroyable;
import net.spy.memcached.*;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Tobias Sarnowski
 */
final class DestroyableMemcachedClient implements MemcachedClientIF, Destroyable {
    private static final Logger LOG = LoggerFactory.getLogger(DestroyableMemcachedClient.class);

    private MemcachedClientIF client;

    DestroyableMemcachedClient(MemcachedClientIF client) {
        this.client = client;
    }

    @Override
    public void destroy() {
        client.shutdown();
    }

    @Override
    public Collection<SocketAddress> getAvailableServers() {
        return client.getAvailableServers();
    }

    @Override
    public Collection<SocketAddress> getUnavailableServers() {
        return client.getUnavailableServers();
    }

    @Override
    public Transcoder<Object> getTranscoder() {
        return client.getTranscoder();
    }

    @Override
    public NodeLocator getNodeLocator() {
        return client.getNodeLocator();
    }

    @Override
    public Future<Boolean> append(long l, String s, Object o) {
        return client.append(l, s, o);
    }

    @Override
    public <T> Future<Boolean> append(long l, String s, T t, Transcoder<T> tTranscoder) {
        return client.append(l, s, t, tTranscoder);
    }

    @Override
    public Future<Boolean> prepend(long l, String s, Object o) {
        return client.prepend(l, s, o);
    }

    @Override
    public <T> Future<Boolean> prepend(long l, String s, T t, Transcoder<T> tTranscoder) {
        return client.prepend(l, s, t, tTranscoder);
    }

    @Override
    public <T> Future<CASResponse> asyncCAS(String s, long l, T t, Transcoder<T> tTranscoder) {
        return client.asyncCAS(s, l, t, tTranscoder);
    }

    @Override
    public Future<CASResponse> asyncCAS(String s, long l, Object o) {
        return client.asyncCAS(s, l, o);
    }

    @Override
    public <T> CASResponse cas(String s, long l, T t, Transcoder<T> tTranscoder) throws OperationTimeoutException {
        return client.cas(s, l, t, tTranscoder);
    }

    @Override
    public CASResponse cas(String s, long l, Object o) throws OperationTimeoutException {
        return client.cas(s, l, o);
    }

    @Override
    public <T> Future<Boolean> add(String s, int i, T t, Transcoder<T> tTranscoder) {
        return client.add(s, i, t, tTranscoder);
    }

    @Override
    public Future<Boolean> add(String s, int i, Object o) {
        return client.add(s, i, o);
    }

    @Override
    public <T> Future<Boolean> set(String s, int i, T t, Transcoder<T> tTranscoder) {
        return client.set(s, i, t, tTranscoder);
    }

    @Override
    public Future<Boolean> set(String s, int i, Object o) {
        return client.set(s, i, o);
    }

    @Override
    public <T> Future<Boolean> replace(String s, int i, T t, Transcoder<T> tTranscoder) {
        return client.replace(s, i, t, tTranscoder);
    }

    @Override
    public Future<Boolean> replace(String s, int i, Object o) {
        return client.replace(s, i, o);
    }

    @Override
    public <T> Future<T> asyncGet(String s, Transcoder<T> tTranscoder) {
        return client.asyncGet(s, tTranscoder);
    }

    @Override
    public Future<Object> asyncGet(String s) {
        return client.asyncGet(s);
    }

    @Override
    public <T> Future<CASValue<T>> asyncGets(String s, Transcoder<T> tTranscoder) {
        return client.asyncGets(s, tTranscoder);
    }

    @Override
    public Future<CASValue<Object>> asyncGets(String s) {
        return client.asyncGets(s);
    }

    @Override
    public <T> CASValue<T> gets(String s, Transcoder<T> tTranscoder) throws OperationTimeoutException {
        return client.gets(s, tTranscoder);
    }

    @Override
    public CASValue<Object> gets(String s) throws OperationTimeoutException {
        return client.gets(s);
    }

    @Override
    public <T> T get(String s, Transcoder<T> tTranscoder) throws OperationTimeoutException {
        return client.get(s, tTranscoder);
    }

    @Override
    public Object get(String s) throws OperationTimeoutException {
        return client.get(s);
    }

    @Override
    public <T> Future<Map<String, T>> asyncGetBulk(Collection<String> strings, Transcoder<T> tTranscoder) {
        return client.asyncGetBulk(strings, tTranscoder);
    }

    @Override
    public Future<Map<String, Object>> asyncGetBulk(Collection<String> strings) {
        return client.asyncGetBulk(strings);
    }

    @Override
    public <T> Future<Map<String, T>> asyncGetBulk(Transcoder<T> tTranscoder, String... strings) {
        return client.asyncGetBulk(tTranscoder, strings);
    }

    @Override
    public Future<Map<String, Object>> asyncGetBulk(String... strings) {
        return client.asyncGetBulk(strings);
    }

    @Override
    public <T> Map<String, T> getBulk(Collection<String> strings, Transcoder<T> tTranscoder) throws OperationTimeoutException {
        return client.getBulk(strings, tTranscoder);
    }

    @Override
    public Map<String, Object> getBulk(Collection<String> strings) throws OperationTimeoutException {
        return client.getBulk(strings);
    }

    @Override
    public <T> Map<String, T> getBulk(Transcoder<T> tTranscoder, String... strings) throws OperationTimeoutException {
        return client.getBulk(tTranscoder, strings);
    }

    @Override
    public Map<String, Object> getBulk(String... strings) throws OperationTimeoutException {
        return client.getBulk(strings);
    }

    @Override
    public Map<SocketAddress, String> getVersions() {
        return client.getVersions();
    }

    @Override
    public Map<SocketAddress, Map<String, String>> getStats() {
        return client.getStats();
    }

    @Override
    public Map<SocketAddress, Map<String, String>> getStats(String s) {
        return client.getStats(s);
    }

    @Override
    public long incr(String s, int i) throws OperationTimeoutException {
        return client.incr(s, i);
    }

    @Override
    public long decr(String s, int i) throws OperationTimeoutException {
        return client.decr(s, i);
    }

    @Override
    public long incr(String s, int i, long l, int i1) throws OperationTimeoutException {
        return client.incr(s, i, l, i1);
    }

    @Override
    public long decr(String s, int i, long l, int i1) throws OperationTimeoutException {
        return client.decr(s, i, l, i1);
    }

    @Override
    public Future<Long> asyncIncr(String s, int i) {
        return client.asyncIncr(s, i);
    }

    @Override
    public Future<Long> asyncDecr(String s, int i) {
        return client.asyncDecr(s, i);
    }

    @Override
    public long incr(String s, int i, long l) throws OperationTimeoutException {
        return client.incr(s, i, l);
    }

    @Override
    public long decr(String s, int i, long l) throws OperationTimeoutException {
        return client.decr(s, i, l);
    }

    @Override
    public Future<Boolean> delete(String s) {
        return client.delete(s);
    }

    @Override
    public Future<Boolean> flush(int i) {
        return client.flush(i);
    }

    @Override
    public Future<Boolean> flush() {
        return client.flush();
    }

    @Override
    public void shutdown() {
        client.shutdown();
    }

    @Override
    public boolean shutdown(long l, TimeUnit timeUnit) {
        return client.shutdown(l, timeUnit);
    }

    @Override
    public boolean waitForQueues(long l, TimeUnit timeUnit) {
        return client.waitForQueues(l, timeUnit);
    }

    @Override
    public boolean addObserver(ConnectionObserver connectionObserver) {
        return client.addObserver(connectionObserver);
    }

    @Override
    public boolean removeObserver(ConnectionObserver connectionObserver) {
        return client.removeObserver(connectionObserver);
    }

    @Override
    public Set<String> listSaslMechanisms() {
        return client.listSaslMechanisms();
    }

    @Override
    public String toString() {
        return "DestroyableMemcachedClient{" +
                "client=" + client +
                '}';
    }
}