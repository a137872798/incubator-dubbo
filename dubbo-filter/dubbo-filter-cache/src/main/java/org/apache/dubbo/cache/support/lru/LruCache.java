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
package org.apache.dubbo.cache.support.lru;

import org.apache.dubbo.cache.Cache;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.LRUCache;

import java.util.Map;

/**
 * LruCache
 *
 * cache的默认实现对象
 */
public class LruCache implements Cache {

    /**
     * 保存缓存对象的容器
     */
    private final Map<Object, Object> store;

    //获取缓存大小
    public LruCache(URL url) {
        final int max = url.getParameter("cache.size", 1000);
        //缓存对象实体 基于 linkedHashMap 这样存入的缓存对象会有先后顺序 可以根据 时间和热度来删除对应元素
        this.store = new LRUCache<Object, Object>(max);
    }

    @Override
    public void put(Object key, Object value) {
        store.put(key, value);
    }

    @Override
    public Object get(Object key) {
        return store.get(key);
    }

}
