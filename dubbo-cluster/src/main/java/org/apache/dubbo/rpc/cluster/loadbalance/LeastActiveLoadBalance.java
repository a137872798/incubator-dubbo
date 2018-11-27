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
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 *
 * 调用活跃次数最小的
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    /**
     * 进行选择
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //获取 可选的长度
        int length = invokers.size(); // Number of invokers
        //最低活跃度默认为-1
        int leastActive = -1; // The least active value of all invokers
        //相同最小活跃数个数
        int leastCount = 0; // The number of invokers having the same least active value (leastActive)
        int[] leastIndexes = new int[length]; // The index of invokers having the same least active value (leastActive)
        int totalWeight = 0; // The sum of with warmup weights
        int firstWeight = 0; // Initial value, used for comparision
        boolean sameWeight = true; // Every invoker has the same weight value?
        //遍历 invoker对象
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            //获取 该方法被调用的 活跃度 可以理解为当前方法 被访问的 并发度
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive(); // Active number
            //通过暖机时间获取权重
            int afterWarmup = getWeight(invoker, invocation);
            //当活跃度更低时  或者是第一次 leastActive 还没被设置时 一旦发现更低活跃度的 就使用这个
            if (leastActive == -1 || active < leastActive) { // Restart, when find a invoker having smaller least active value.
                //设置成 当前invoker 的活跃度
                leastActive = active; // Record the current least active value
                leastCount = 1; // Reset leastCount, count again based on current leastCount
                leastIndexes[0] = i; // Reset
                totalWeight = afterWarmup; // Reset
                firstWeight = afterWarmup; // Record the weight the first invoker
                sameWeight = true; // Reset, every invoker has the same weight value?
                //当前活跃度 跟 最小相同时
            } else if (active == leastActive) { // If current invoker's active value equals with leaseActive, then accumulating.
                //活跃度相同时增加 相同 活跃度的对象
                leastIndexes[leastCount++] = i; // Record index number of this invoker
                totalWeight += afterWarmup; // Add this invoker's with warmup weight to totalWeight.
                // If every invoker has the same weight?
                if (sameWeight && i > 0
                        && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // assert(leastCount > 0) 直接返回active 最小的 元素
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);
        }
        //如果权重发生了变化  代表活跃度相同的情况下 有某个对象的 权重不一样 如果活跃度存在最低值 直接返回那个对象就好了
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            //受权重影响 所以以这种方式 这样权重高的 就容易被分配 这里隐式条件就是活跃度也一样
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight) + 1;
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= getWeight(invokers.get(leastIndex), invocation);
                if (offsetWeight <= 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        //权重都一样直接返回随机数 这里隐式条件就是活跃度也一样
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
