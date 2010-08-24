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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import de.cosmocode.palava.ipc.Current;
import de.cosmocode.palava.ipc.IpcConnectionScoped;
import de.cosmocode.palava.ipc.cache.CacheFilterOnlyModule;
import de.cosmocode.palava.ipc.cache.CommandCacheService;
import net.spy.memcached.MemcachedClientIF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tobias Sarnowski
 */
public final class MemcacheFilterModule implements Module {
    private static final Logger LOG = LoggerFactory.getLogger(MemcacheFilterModule.class);

    @Override
    public void configure(Binder binder) {
        binder.install(new CacheFilterOnlyModule());
        binder.bind(CommandCacheService.class).to(MemcacheService.class).in(Singleton.class);
        binder.bind(MemcachedClientIF.class)
                .annotatedWith(Current.class)
                .toProvider(MemcacheService.class)
                .in(IpcConnectionScoped.class);
    }
}