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

import java.util.Map;

import com.google.common.collect.Maps;

import de.cosmocode.palava.ipc.IpcArguments;
import de.cosmocode.palava.ipc.IpcCommand;
import de.cosmocode.palava.ipc.MapIpcArguments;
import de.cosmocode.palava.ipc.cache.CacheKey;
import de.cosmocode.rendering.Renderable;
import de.cosmocode.rendering.Renderer;
import de.cosmocode.rendering.RenderingException;
import de.cosmocode.rendering.RenderingLevel;

/**
 * A json based {@link CacheKey}.
 * 
 * @author Tobias Sarnowski
 */
public class JsonCacheKey implements CacheKey, Renderable {

    private static final long serialVersionUID = 701148429090791480L;
    
    private Class<? extends IpcCommand> command;
    private Map<String, Object> arguments;

    public JsonCacheKey(Class<? extends IpcCommand> command, IpcArguments arguments) {
        this.command = command;
        this.arguments = Maps.newHashMap(arguments);
    }

    @Override
    public Class<? extends IpcCommand> getCommand() {
        return command;
    }

    @Override
    public IpcArguments getArguments() {
        return new MapIpcArguments(arguments);
    }

    @Override
    public void render(Renderer r, RenderingLevel level) throws RenderingException {
        r.key("cmd").value(command.getName());
        r.key("args").value(arguments);
    }

    @Override
    public String toString() {
        return "JsonCacheKey [command=" + command + ", arguments=" + arguments + "]";
    }

}
