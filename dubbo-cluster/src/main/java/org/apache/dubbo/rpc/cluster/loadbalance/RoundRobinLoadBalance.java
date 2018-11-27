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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 *
 * 轮询
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";
    
    private static int RECYCLE_PERIOD = 60000;

    /**
     * 计算权重
     */
    protected static class WeightedRoundRobin {
        private int weight;
        /**
         * 记录下一次重置前的 权重
         */
        private AtomicLong current = new AtomicLong(0);
        private long lastUpdate;
        public int getWeight() {
            return weight;
        }

        /**
         * 设置权重的时候 重置 current
         * @param weight
         */
        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        /**
         * 增加权重值
         * @return
         */
        public long increaseCurrent() {
            return current.addAndGet(weight);
        }
        public void sel(int total) {
            current.addAndGet(-1 * total);
        }
        public long getLastUpdate() {
            return lastUpdate;
        }
        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * key1 服务键+methodname  key2 url对象转换的string
     */
    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();
    private AtomicBoolean updateLock = new AtomicBoolean();
    
    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     * 
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    /**
     * 选择的 核心逻辑
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //将服务键 拼接 方法
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        //获取二级容器
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map == null) {
            methodWeightMap.putIfAbsent(key, new ConcurrentHashMap<String, WeightedRoundRobin>());
            map = methodWeightMap.get(key);
        }
        int totalWeight = 0;
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null;
        //创建轮询权重对象
        WeightedRoundRobin selectedWRR = null;
        //遍历 invoker 对象
        for (Invoker<T> invoker : invokers) {
            //将url 转换成string对象
            String identifyString = invoker.getUrl().toIdentityString();
            //从二级容器中获取 权重对象
            WeightedRoundRobin weightedRoundRobin = map.get(identifyString);
            //获取该invoker 的权重值
            int weight = getWeight(invoker, invocation);
            if (weight < 0) {
                weight = 0;
            }
            if (weightedRoundRobin == null) {
                weightedRoundRobin = new WeightedRoundRobin();
                //第一次 针对该invoker 对象设置 权重
                weightedRoundRobin.setWeight(weight);
                map.putIfAbsent(identifyString, weightedRoundRobin);
                weightedRoundRobin = map.get(identifyString);
            }
            //第二次开始的权重 与 第一次设置的权重是否相同  每次 weight的 权重应该会变化
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                weightedRoundRobin.setWeight(weight);
            }
            //在setWeight 后权重为0 这里 将权重与 current 值同步
            long cur = weightedRoundRobin.increaseCurrent();
            //设置最后更新时间
            weightedRoundRobin.setLastUpdate(now);
            if (cur > maxCurrent) {
                //更新 maxCurrent 这个值 是针对每个invoker对象的  这样会设置成权重最高的对象
                maxCurrent = cur;
                //使用当前invoker
                selectedInvoker = invoker;
                //被选中的 wrr对象
                selectedWRR = weightedRoundRobin;
            }
            //增加权重 当遍历完每个 invoker 后这个就是 总权重
            totalWeight += weight;
        }
        //这里应该是并发调用了
        if (!updateLock.get() && invokers.size() != map.size()) {
            //代表成功抢占线程
            if (updateLock.compareAndSet(false, true)) {
                try {
                    // copy -> modify -> update reference
                    ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<String, WeightedRoundRobin>();
                    newMap.putAll(map);
                    Iterator<Entry<String, WeightedRoundRobin>> it = newMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, WeightedRoundRobin> item = it.next();
                        if (now - item.getValue().getLastUpdate() > RECYCLE_PERIOD) {
                            //有些失效了 就移除
                            it.remove();
                        }
                    }
                    methodWeightMap.put(key, newMap);
                } finally {
                    updateLock.set(false);
                }
            }
        }
        //如果选择到了对象 这时选择的是权重最高的对象
        if (selectedInvoker != null) {
            //将current 属性 变成负数 这样在下次 setWeight 前 权重都会比较小 每次
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(0);
    }

}
