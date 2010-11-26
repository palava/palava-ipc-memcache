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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import de.cosmocode.commons.reflect.Classpath;
import de.cosmocode.commons.reflect.Packages;
import de.cosmocode.commons.reflect.Reflection;
import de.cosmocode.jackson.JacksonRenderer;
import de.cosmocode.palava.core.lifecycle.Initializable;
import de.cosmocode.palava.core.lifecycle.LifecycleException;
import de.cosmocode.palava.ipc.Current;
import de.cosmocode.palava.ipc.IpcCall;
import de.cosmocode.palava.ipc.IpcCallFilterChain;
import de.cosmocode.palava.ipc.IpcCommand;
import de.cosmocode.palava.ipc.IpcCommandExecutionException;
import de.cosmocode.palava.ipc.cache.CacheKey;
import de.cosmocode.palava.ipc.cache.CacheKeyFactory;
import de.cosmocode.palava.ipc.cache.CachePolicy;
import de.cosmocode.palava.ipc.cache.CommandCacheService;
import de.cosmocode.rendering.Renderer;

/**
 * @author Tobias Sarnowski
 */
final class MemcacheService implements CommandCacheService, Provider<MemcachedClientIF>, Initializable {
    private static final Logger LOG = LoggerFactory.getLogger(MemcacheService.class);

    private final List<InetSocketAddress> addresses;
    private boolean binary = false;
    private int defaultTimeout = 0;
    private TimeUnit defaultTimeoutUnit = TimeUnit.SECONDS;
    private int compressionThreshold = -1;
    private HashAlgorithm hashAlgorithm = HashAlgorithm.NATIVE_HASH;

    private final CacheContainer cacheContainer;
    private final Provider<MemcachedClientIF> memcachedClientProvider;
    private final String packages;

    @Inject
    public MemcacheService(
            @Named(MemcacheConfig.ADRESSES) String addresses,
            @IpcMemcache CacheContainer cacheContainer,
            @Current Provider<MemcachedClientIF> memcachedClientProvider,
            @Named(MemcacheConfig.PACKAGES) String packages) {
        this.cacheContainer = cacheContainer;
        this.memcachedClientProvider = memcachedClientProvider;
        this.packages = packages;
        Preconditions.checkNotNull(addresses, "Addresses");
        this.addresses = AddrUtil.getAddresses(addresses);
    }

    @Override
    public void initialize() throws LifecycleException {
        final Classpath classpath = Reflection.defaultClasspath();
        final Packages packages = classpath.restrictTo(this.packages.split(","));

        LOG.info("Preloading command caches in {}", this.packages);
        for (Class<? extends IpcCommand> type : packages.subclassesOf(IpcCommand.class)) {
            // preload caches
            cacheContainer.getCache(type.getName());
        }
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

    @Inject(optional = true)
    public void setCompressionThreshold(@Named(MemcacheConfig.COMPRESSION_THRESHOLD) int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    @Inject(optional = true)
    public void setHashAlgorithm(@Named(MemcacheConfig.HASH_ALGORITHM) HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    @Override
    public MemcachedClientIF get() {
        try {
            final ConnectionFactory cf;
            if (binary) {
                cf = new BinaryConnectionFactory(
                        BinaryConnectionFactory.DEFAULT_OP_QUEUE_LEN,
                        BinaryConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
                        hashAlgorithm
                );
            } else {
                cf = new DefaultConnectionFactory(
                        DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN,
                        DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
                        hashAlgorithm
                );
            }
            final MemcachedClient client = new MemcachedClient(cf, addresses);

            if (compressionThreshold >= 0) {
                if (client.getTranscoder() instanceof BaseSerializingTranscoder) {
                    BaseSerializingTranscoder bst = (BaseSerializingTranscoder)client.getTranscoder();
                    bst.setCompressionThreshold(compressionThreshold);
                } else {
                    throw new UnsupportedOperationException("cannot set compression threshold; transcoder does not extend BaseSeralizingTranscode");
                }
            }

            return new DestroyableMemcachedClient(client);
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

        final Cache<CacheKey,Boolean> cache = cacheContainer.getCache(command.getName());

        if (cache.isEmpty()) {
            LOG.trace("No cached versions of {} found.", command);
        } else {
            LOG.trace("Trying to invalidate {} cached versions of {}...", cache.size(), command);

            // infinispan uses immutable iterators, so no: iterator.remove();

            Set<CacheKey> keys = Sets.newHashSet();
            for (CacheKey cacheKey: cache.keySet()) {
                if (predicate.apply(cacheKey)) {
                    LOG.debug("{} matches {}, invalidating...", cacheKey, predicate);
                    keys.add(cacheKey);
                } else {
                    LOG.trace("{} does not match {}", cacheKey, predicate);
                }
            }

            LOG.debug("invalidating found keys...");
            for (CacheKey cacheKey: keys) {
                final Renderer rKey = new JacksonRenderer();
                final String key = rKey.value(cacheKey).build().toString();
                memcache.delete(key);
                cache.removeAsync(cacheKey);
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
        // the key set of this command
        final Cache<CacheKey,Boolean> cache = cacheContainer.getCache(command.getClass().getName());

        // add the cache key
        cache.putIfAbsentAsync(cacheKey, true);

        LOG.debug("Added {} to index {}", cacheKey, cache);
    }
}
