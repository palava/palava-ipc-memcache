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
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.MemcachedClientIF;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.cosmocode.commons.Strings;
import de.cosmocode.commons.reflect.Classpath;
import de.cosmocode.commons.reflect.Packages;
import de.cosmocode.commons.reflect.Reflection;
import de.cosmocode.jackson.JacksonRenderer;
import de.cosmocode.palava.cache.AbstractComputingCacheService;
import de.cosmocode.palava.cache.CacheExpiration;
import de.cosmocode.palava.core.lifecycle.Initializable;
import de.cosmocode.palava.core.lifecycle.LifecycleException;
import de.cosmocode.palava.ipc.Current;
import de.cosmocode.palava.ipc.IpcCall;
import de.cosmocode.palava.ipc.IpcCommand;
import de.cosmocode.palava.ipc.IpcCommandExecutionException;
import de.cosmocode.palava.ipc.cache.CacheKey;
import de.cosmocode.palava.ipc.cache.IpcCacheService;
import de.cosmocode.palava.ipc.cache.IpcCommandExecution;
import de.cosmocode.rendering.Renderer;

/**
 * A Memcache based {@link CommandCacheService} implementation.
 * 
 * @author Tobias Sarnowski
 */
final class MemcacheService extends AbstractComputingCacheService implements IpcCacheService, Initializable {
    
    private static final Logger LOG = LoggerFactory.getLogger(MemcacheService.class);

    private final CacheContainer container;
    private final MemcachedClientIF client;
    private final String packages;
    private final ObjectMapper mapper;
    
    @Inject
    public MemcacheService(
            @IpcMemcache CacheContainer container,
            @Current MemcachedClientIF client,
            @Named(MemcacheConfig.PACKAGES) String packages,
            ObjectMapper mapper) {
        this.container = Preconditions.checkNotNull(container, "Container");
        this.client = Preconditions.checkNotNull(client, "Client");
        this.packages = Preconditions.checkNotNull(packages, "Packages");
        this.mapper = Preconditions.checkNotNull(mapper, "Mapper");
    }

    @Override
    public void initialize() throws LifecycleException {
        final Classpath classpath = Reflection.defaultClasspath();
        final Packages packageList = classpath.restrictTo(packages.split(","));

        LOG.info("Preloading command caches in {}", packageList);
        for (Class<? extends IpcCommand> command : packageList.subclassesOf(IpcCommand.class)) {
            LOG.trace("Preloading index cache for {}", command);
            container.getCache(command.getName());
        }
    }

    private CacheKey createKey(IpcCommand command, IpcCall call) {
        return new JsonCacheKey(command.getClass(), call.getArguments());
    }
    
    private String encode(Object content) {
        final Renderer r = new JacksonRenderer();
        return r.value(content).build().toString();
    }
    
    private <T> T checkType(Object input, Class<T> type) {
        Preconditions.checkArgument(type.isInstance(input), "Expected %s to be of type %s", input, type);
        return type.cast(input);
    }
    
    @Override
    protected void doStore(Serializable rawKey, Object rawValue, CacheExpiration expiration) {
        final CacheKey cacheKey = checkType(rawKey, CacheKey.class);
        final Map<?, ?> cacheValue = checkType(rawValue, Map.class);
        
        final String key = encode(cacheKey);
        final int timeout = (int) expiration.getLifeTimeIn(TimeUnit.SECONDS);
        final String value = encode(cacheValue);
        
        client.set(key, timeout, value);
        
        // update index
        final Cache<CacheKey, Boolean> cache = container.getCache(cacheKey.getCommand().getName());
        cache.putIfAbsentAsync(cacheKey, Boolean.TRUE);
        LOG.debug("Added {} to index {}", cacheKey, cache);
    }

    @Override
    protected <V> V doRead(Serializable rawKey) {
        final String key = encode(checkType(rawKey, CacheKey.class));
        final String value = Strings.toString(client.get(key));
        try {
            if (value == null) {
                return null;
            } else {
                @SuppressWarnings("unchecked")
                final V map = (V) mapper.readValue(value, Map.class);
                return map;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected <V> V doRemove(Serializable key) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doClear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> read(IpcCommand command, IpcCall call) {
        return doRead(createKey(command, call));
    }

    @Override
    public Map<String, Object> computeAndStore(IpcCommand command, IpcCall call, CacheExpiration expiration,
            IpcCommandExecution computation) throws IpcCommandExecutionException {
        
        try {
            final CacheKey key = createKey(command, call);
            return computeAndStore(key, computation, expiration);
        } catch (ExecutionException e) {
            throw new IpcCommandExecutionException(e.getCause());
        }
    }

    @Override
    public void invalidate(Class<? extends IpcCommand> command) {
        invalidate(command, Predicates.alwaysTrue());
    }

    @Override
    public void invalidate(Class<? extends IpcCommand> command, Predicate<? super CacheKey> predicate) {
        Preconditions.checkNotNull(command, "Command");
        Preconditions.checkNotNull(predicate, "Predicate");
    
        final Cache<CacheKey, Boolean> cache = container.getCache(command.getName());
    
        if (cache.isEmpty()) {
            LOG.trace("No cached versions of {} found.", command);
        } else {
            LOG.trace("Trying to invalidate {} cached versions of {}...", cache.size(), command);
            LOG.debug("invalidating found keys...");
            for (CacheKey cacheKey : Iterables.filter(cache.keySet(), predicate)) {
                final String key = encode(cacheKey);
                client.delete(key);
                cache.removeAsync(cacheKey);
            }
        }
    }
    
}
