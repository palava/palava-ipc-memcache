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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import de.cosmocode.jackson.JacksonRenderer;
import de.cosmocode.palava.ipc.*;
import de.cosmocode.palava.ipc.cache.*;
import de.cosmocode.rendering.Renderer;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Tobias Sarnowski
 */
final class MemcacheService implements CommandCacheService, Provider<MemcachedClientIF> {
    private static final Logger LOG = LoggerFactory.getLogger(MemcacheService.class);

    private final List<InetSocketAddress> addresses;
    private boolean binary = false;
    private int defaultTimeout = 1;
    private TimeUnit defaultTimeoutUnit = TimeUnit.HOURS;

    private final Provider<MemcachedClientIF> memcachedClientProvider;

    @Inject
    public MemcacheService(
            @Named(MemcacheConfig.ADRESSES) String addresses,
            @Current Provider<MemcachedClientIF> memcachedClientProvider) {
        this.memcachedClientProvider = memcachedClientProvider;
        Preconditions.checkNotNull(addresses, "Addresses");
        this.addresses = AddrUtil.getAddresses(addresses);
    }

    @Inject(optional = true)
    public void setBinary(@Named(MemcacheConfig.BINARY) boolean binary) {
        this.binary = binary;
    }

    @Inject(optional = true)
    public void setDefaultTimeout(@Named(MemcacheConfig.DEFAULT_TIMEOUT) int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    @Inject(optional = true)
    public void setDefaultTimeoutUnit(@Named(MemcacheConfig.DEFAULT_TIMEOUT_UNIT) TimeUnit defaultTimeoutUnit) {
        this.defaultTimeoutUnit = defaultTimeoutUnit;
    }

    @Override
    public MemcachedClientIF get() {
        try {
            if (binary) {
                return new DestroyableMemcachedClient(
                        new MemcachedClient(new BinaryConnectionFactory(), addresses)
                );
            } else {
                return new MemcachedClient(addresses);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void setFactory(CacheKeyFactory factory) {
        throw new UnsupportedOperationException("memcache does not support this");
    }

    @Override
    public void invalidate(Class<? extends IpcCommand> command) {
        invalidate(command, Predicates.alwaysTrue());
    }

    @Override
    public void invalidate(Class<? extends IpcCommand> command, Predicate<? super CacheKey> predicate) {
        Preconditions.checkNotNull(command, "Command");
        Preconditions.checkNotNull(predicate, "Predicate");

        MemcachedClientIF memcache = memcachedClientProvider.get();

        final String indexKey = command.getClass().getName();
        final Set<CacheKey> index = (Set<CacheKey>)memcache.get(indexKey);

        if (index == null) {
            LOG.trace("No cached versions of {} found.", command);
        } else {
            LOG.trace("Trying to invalidate {} cached versions of {}...", index.size(), command);

            final Iterator<CacheKey> iterator = index.iterator();

            while (iterator.hasNext()) {
                final CacheKey cacheKey = iterator.next();
                if (predicate.apply(cacheKey)) {
                    LOG.trace("{} matches {}, invalidating...", cacheKey, predicate);
                    final Renderer rKey = new JacksonRenderer();
                    final String key = rKey.value(cacheKey).build().toString();
                    memcache.delete(key);
                    iterator.remove();
                } else {
                    LOG.trace("{} does not match {}", cacheKey, predicate);
                }
            }

            if (index.isEmpty()) {
                LOG.trace("Removing empty index for {}", command);
                memcache.delete(indexKey);
            } else {
                LOG.trace("Updating index for {}", command);
                memcache.set(indexKey, 0, index);
            }
        }
    }

    @Override
    public Map<String, Object> cache(IpcCall call, IpcCommand command, IpcCallFilterChain chain, CachePolicy policy) throws IpcCommandExecutionException {
        return cache(call, command, chain, policy, 0, TimeUnit.SECONDS);
    }

    @Override
    public Map<String, Object> cache(IpcCall call, IpcCommand command, IpcCallFilterChain chain, CachePolicy policy, long maxAge, TimeUnit maxAgeUnit) throws IpcCommandExecutionException {
        if (policy != CachePolicy.SMART) {
            throw new UnsupportedOperationException("Memcache does only support SMART policy [cachePolicy= " + policy.name() + " @ " + command.getClass().getName() + "]");
        }

        // execute the command
        final Map<String, Object> result = chain.filter(call, command);

        // calculate timeout
        if (maxAge == 0) {
            maxAge = defaultTimeout;
            maxAgeUnit = defaultTimeoutUnit;
        }
        final int timeout = (int)maxAgeUnit.toSeconds(maxAge);

        // get the memcache connection
        MemcachedClientIF memcache = memcachedClientProvider.get();

        // generate the json
        final Renderer rKey = new JacksonRenderer();
        final CacheKey cacheKey = new JsonCacheKey(command.getClass(), call.getArguments());
        final String key = rKey.value(cacheKey).build().toString();

        final Renderer rValue = new JacksonRenderer();
        final String value = rValue.value(result).build().toString();

        // store it
        LOG.trace("Storing {} => {}..", key, value);
        memcache.set(key, timeout, value);
        updateIndex(command, cacheKey);

        // return the result
        return result;
    }

    private void updateIndex(IpcCommand command, CacheKey cacheKey) {
        MemcachedClientIF memcache = memcachedClientProvider.get();
        final String indexKey = command.getClass().getName();

        Set<CacheKey> index = (Set<CacheKey>)memcache.get(indexKey);
        if (index == null) {
            index = Sets.newHashSet();
        }

        index.add(cacheKey);
        memcache.set(indexKey, 0, index);

        LOG.trace("Cached {} in memcache", cacheKey);
    }
}