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
package org.apache.dubbo.rpc.filter.tps;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 默认的限流对象
 */
public class DefaultTPSLimiter implements TPSLimiter {

    /**
     * key 服务名 value 统计对象
     */
    private final ConcurrentMap<String, StatItem> stats
            = new ConcurrentHashMap<String, StatItem>();

    /**
     * 判断是否需要限流
     * @param url        url
     * @param invocation invocation
     * @return
     */
    @Override
    public boolean isAllowable(URL url, Invocation invocation) {
        //判断是否存在 tps 相关限制 rate代表最大允许的 请求量
        int rate = url.getParameter(Constants.TPS_LIMIT_RATE_KEY, -1);
        //判断是否存在 tps.interval 属性
        long interval = url.getParameter(Constants.TPS_LIMIT_INTERVAL_KEY,
                Constants.DEFAULT_TPS_LIMIT_INTERVAL);
        //获取url 的服务键内容  group:interface:version
        String serviceKey = url.getServiceKey();
        if (rate > 0) {
            //通过服务键 获取 统计信息对象
            StatItem statItem = stats.get(serviceKey);
            if (statItem == null) {
                //初始化 统计对象
                stats.putIfAbsent(serviceKey,
                        new StatItem(serviceKey, rate, interval));
                statItem = stats.get(serviceKey);
            }
            //委托判断是否允许  也就是当rate 为0的时候不允许调用 通过cas 和 自旋完成类似 信号量的功能  2者对比有什么优缺点
            return statItem.isAllowable();
        } else {
            //代表不需要 该对象
            StatItem statItem = stats.get(serviceKey);
            if (statItem != null) {
                stats.remove(serviceKey);
            }
        }

        return true;
    }

}
