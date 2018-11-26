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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于限流的 统计信息对象
 */
class StatItem {

    /**
     * 服务名
     */
    private String name;

    /**
     * 最后重置时间
     */
    private long lastResetTime;

    /**
     * 时间间隔
     */
    private long interval;

    /**
     * 好像会定期变成 rate 的值
     */
    private AtomicInteger token;

    /**
     * 比率
     */
    private int rate;

    StatItem(String name, int rate, long interval) {
        this.name = name;
        this.rate = rate;
        this.interval = interval;
        this.lastResetTime = System.currentTimeMillis();
        this.token = new AtomicInteger(rate);
    }

    /**
     * 判断是否允许
     * @return
     */
    public boolean isAllowable() {
        long now = System.currentTimeMillis();
        //代表需要重置了
        if (now > lastResetTime + interval) {
            //更新token 的值
            token.set(rate);
            lastResetTime = now;
        }

        //获取当前的 rate 值 可能更新了 也可能没更新
        int value = token.get();
        boolean flag = false;
        while (value > 0 && !flag) {
            //直到修改成功为止 就是将 value - 1
            flag = token.compareAndSet(value, value - 1);
            value = token.get();
        }

        //一般都会是0 当 value <= 0的时候就不会在处理了 value 可以说就是从 rate 中取出来的
        return flag;
    }

    long getLastResetTime() {
        return lastResetTime;
    }

    int getToken() {
        return token.get();
    }

    @Override
    public String toString() {
        return new StringBuilder(32).append("StatItem ")
                .append("[name=").append(name).append(", ")
                .append("rate = ").append(rate).append(", ")
                .append("interval = ").append(interval).append("]")
                .toString();
    }

}
