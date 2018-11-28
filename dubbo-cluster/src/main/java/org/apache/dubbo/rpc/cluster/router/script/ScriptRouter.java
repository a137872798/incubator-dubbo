/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.router.script;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ScriptRouter
 *
 * 脚本实现的路由对象
 */
public class ScriptRouter implements Router {

    private static final Logger logger = LoggerFactory.getLogger(ScriptRouter.class);

    /**
     * key 脚本类型 value 脚本引擎 脚本引擎对象是 jdk自带的
     */
    private static final Map<String, ScriptEngine> engines = new ConcurrentHashMap<String, ScriptEngine>();

    /**
     * 引擎对象
     */
    private final ScriptEngine engine;

    private final int priority;

    /**
     * 规则内容
     */
    private final String rule;

    /**
     * 路由规则 url
     */
    private final URL url;

    public ScriptRouter(URL url) {
        this.url = url;
        //获取 type  这个和 rule 属性都是从FileRouteFactory 中设置的 并传入到下面的路由对象
        String type = url.getParameter(Constants.TYPE_KEY);
        this.priority = url.getParameter(Constants.PRIORITY_KEY, 0);
        //获取rule 信息
        String rule = url.getParameterAndDecoded(Constants.RULE_KEY);
        //脚本类型不存在就默认是 javascript
        if (type == null || type.length() == 0) {
            type = Constants.DEFAULT_SCRIPT_TYPE_KEY;
        }
        //不存在路由规则会抛出异常
        if (rule == null || rule.length() == 0) {
            throw new IllegalStateException(new IllegalStateException("route rule can not be empty. rule:" + rule));
        }
        //通过 容器获得 对应类型的 引擎对象
        ScriptEngine engine = engines.get(type);
        if (engine == null) {
            //从 引擎管理对象中获取对应对象 这个对象是 jdk携带的
            engine = new ScriptEngineManager().getEngineByName(type);
            if (engine == null) {
                throw new IllegalStateException(new IllegalStateException("Unsupported route rule type: " + type + ", rule: " + rule));
            }
            engines.put(type, engine);
        }
        this.engine = engine;
        this.rule = rule;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    /**
     * 路由信息
     * @param invokers
     * @param url        refer url
     * @param invocation
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        try {
            List<Invoker<T>> invokersCopy = new ArrayList<Invoker<T>>(invokers);
            Compilable compilable = (Compilable) engine;
            Bindings bindings = engine.createBindings();
            bindings.put("invokers", invokersCopy);
            bindings.put("invocation", invocation);
            bindings.put("context", RpcContext.getContext());
            CompiledScript function = compilable.compile(rule);
            Object obj = function.eval(bindings);
            if (obj instanceof Invoker[]) {
                invokersCopy = Arrays.asList((Invoker<T>[]) obj);
            } else if (obj instanceof Object[]) {
                invokersCopy = new ArrayList<Invoker<T>>();
                for (Object inv : (Object[]) obj) {
                    invokersCopy.add((Invoker<T>) inv);
                }
            } else {
                invokersCopy = (List<Invoker<T>>) obj;
            }
            return invokersCopy;
        } catch (ScriptException e) {
            //fail then ignore rule .invokers.
            logger.error("route error , rule has been ignored. rule: " + rule + ", method:" + invocation.getMethodName() + ", url: " + RpcContext.getContext().getUrl(), e);
            return invokers;
        }
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != ScriptRouter.class) {
            return 1;
        }
        ScriptRouter c = (ScriptRouter) o;
        return this.priority == c.priority ? rule.compareTo(c.rule) : (this.priority > c.priority ? 1 : -1);
    }

}
