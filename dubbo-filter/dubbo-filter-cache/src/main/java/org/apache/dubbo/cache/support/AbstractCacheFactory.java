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
package org.apache.dubbo.cache.support;

import org.apache.dubbo.cache.Cache;
import org.apache.dubbo.cache.CacheFactory;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * AbstractCacheFactory
 *
 * 抽象缓存工厂实现
 */
public abstract class AbstractCacheFactory implements CacheFactory {

    /**
     * 缓存容器对象
     */
    private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<String, Cache>();

    /**
     * 获取缓存对象
     * @param url
     * @param invocation
     * @return
     */
    @Override
    public Cache getCache(URL url, Invocation invocation) {
        //这里 会返回一个新的url 对象 并设置了 method属性
        url = url.addParameter(Constants.METHOD_KEY, invocation.getMethodName());
        //将url 转换为key 对象
        String key = url.toFullString();
        //尝试获取cache 对象
        Cache cache = caches.get(key);
        if (cache == null) {
            //没有就 通过url 创建 cache对象
            caches.put(key, createCache(url));
            cache = caches.get(key);
        }
        return cache;
    }

    protected abstract Cache createCache(URL url);

}
