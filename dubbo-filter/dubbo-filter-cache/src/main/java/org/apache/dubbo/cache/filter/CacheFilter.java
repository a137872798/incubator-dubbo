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
package org.apache.dubbo.cache.filter;

import org.apache.dubbo.cache.Cache;
import org.apache.dubbo.cache.CacheFactory;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;

import java.io.Serializable;

/**
 * CacheFilter
 *
 * 可以针对某个方法 或接口 进行缓存 处理  实现缓存的核心类 还是cacheFactory
 */
@Activate(group = {Constants.CONSUMER, Constants.PROVIDER}, value = Constants.CACHE_KEY)
public class CacheFilter implements Filter {

    /**
     * 缓存工厂对象
     */
    private CacheFactory cacheFactory;

    /**
     * 设置缓存工厂
     * @param cacheFactory
     */
    public void setCacheFactory(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //存在缓存工厂 并且 存在cache 属性
        if (cacheFactory != null && ConfigUtils.isNotEmpty(invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.CACHE_KEY))) {
            //通过工厂生成 cache 对象
            Cache cache = cacheFactory.getCache(invoker.getUrl(), invocation);
            if (cache != null) {
                //将参数 拼接成字符串
                String key = StringUtils.toArgumentString(invocation.getArguments());
                //从缓存中获取结果对象
                Object value = cache.get(key);
                if (value != null) {
                    //如果结果对象是包装类
                    if (value instanceof ValueWrapper) {
                        //取出包装类中的值 封装成result对象 并返回
                        return new RpcResult(((ValueWrapper)value).get());
                    } else {
                        //直接返回
                        return new RpcResult(value);
                    }
                }
                //不存在缓存的情况下 直接调用
                Result result = invoker.invoke(invocation);
                if (!result.hasException()) {
                    //没有出现异常的情况将 返回结果包装并 存入 缓存中
                    cache.put(key, new ValueWrapper(result.getValue()));
                }
                return result;
            }
        }
        return invoker.invoke(invocation);
    }

    /**
     * 缓存结果包装对象
     */
    static class ValueWrapper implements Serializable{

        private static final long serialVersionUID = -1777337318019193256L;

        private final Object value;

        public ValueWrapper(Object value){
            this.value = value;
        }

        public Object get() {
            return this.value;
        }
    }
}
