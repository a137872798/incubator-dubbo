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
package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.common.utils.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Request.
 */
public class Request {

    /**
     * 心跳事件 不传递任何事件类型就代表是 心跳事件
     */
    public static final String HEARTBEAT_EVENT = null;

    /**
     * 只读事件
     */
    public static final String READONLY_EVENT = "R";

    /**
     * 自增请求 序列
     */
    private static final AtomicLong INVOKE_ID = new AtomicLong(0);

    /**
     * 当前的 请求编号
     */
    private final long mId;

    /**
     * 当前版本
     */
    private String mVersion;

    /**
     * 是否 需要返回相应结果  (是否是 oneway请求)
     */
    private boolean mTwoWay = true;

    /**
     * 是否是 事件
     */
    private boolean mEvent = false;

    /**
     * 是否是异常的请求
     */
    private boolean mBroken = false;

    /**
     * 数据实体
     */
    private Object mData;

    /**
     * 初始化时  就创建本次请求的 id
     */
    public Request() {
        mId = newId();
    }

    /**
     * 通过传入指定的 id 初始化请求
     * @param id
     */
    public Request(long id) {
        mId = id;
    }

    /**
     * 调用自增序列
     * @return
     */
    private static long newId() {
        // getAndIncrement() When it grows to MAX_VALUE, it will grow to MIN_VALUE, and the negative can be used as ID
        return INVOKE_ID.getAndIncrement();
    }

    /**
     * 将 传入的data 转换为 string 类型
     * @param data
     * @return
     */
    private static String safeToString(Object data) {
        if (data == null) {
            return null;
        }
        String dataStr;
        try {
            dataStr = data.toString();
        } catch (Throwable e) {
            dataStr = "<Fail toString of " + data.getClass() + ", cause: " +
                    StringUtils.toString(e) + ">";
        }
        return dataStr;
    }

    public long getId() {
        return mId;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        mVersion = version;
    }

    public boolean isTwoWay() {
        return mTwoWay;
    }

    public void setTwoWay(boolean twoWay) {
        mTwoWay = twoWay;
    }

    public boolean isEvent() {
        return mEvent;
    }

    public void setEvent(String event) {
        mEvent = true;
        mData = event;
    }

    public boolean isBroken() {
        return mBroken;
    }

    public void setBroken(boolean mBroken) {
        this.mBroken = mBroken;
    }

    public Object getData() {
        return mData;
    }

    public void setData(Object msg) {
        mData = msg;
    }

    /**
     * 判断是否是 心跳检测事件
     * @return
     */
    public boolean isHeartbeat() {
        //存在 事件 并且 data 为null
        return mEvent && HEARTBEAT_EVENT == mData;
    }

    /**
     * 设置 心跳事件
     * @param isHeartbeat
     */
    public void setHeartbeat(boolean isHeartbeat) {
        if (isHeartbeat) {
            setEvent(HEARTBEAT_EVENT);
        }
    }

    @Override
    public String toString() {
        return "Request [id=" + mId + ", version=" + mVersion + ", twoway=" + mTwoWay + ", event=" + mEvent
                + ", broken=" + mBroken + ", data=" + (mData == this ? "this" : safeToString(mData)) + "]";
    }
}
