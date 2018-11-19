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
package org.apache.dubbo.rpc;

import java.io.Serializable;
import java.util.Map;


/**
 * RPC invoke result. (API, Prototype, NonThreadSafe)
 *
 * @serial Don't change the class name and package name.
 * @see org.apache.dubbo.rpc.Invoker#invoke(Invocation)
 * @see org.apache.dubbo.rpc.RpcResult
 */

/**
 * 调用invoker 返回的结果
 */
public interface Result extends Serializable {

    /**
     * Get invoke result.
     *
     * @return result. if no result return null.
     */
    //返回的 值对象
    Object getValue();

    /**
     * Get exception.
     *
     * @return exception. if no exception return null.
     */
    //返回的异常对象
    Throwable getException();

    /**
     * Has exception.
     *
     * @return has exception.
     */
    //是否存在异常
    boolean hasException();

    /**
     * Recreate.
     * <p>
     * <code>
     * if (hasException()) {
     * throw getException();
     * } else {
     * return getValue();
     * }
     * </code>
     *
     * @return result.
     * @throws if has exception throw it.
     */
    //看不懂 好像就是 有异常 返回异常 没有就返回结果对象
    Object recreate() throws Throwable;

    /**
     * @see org.apache.dubbo.rpc.Result#getValue()
     * @deprecated Replace to getValue()
     */
    @Deprecated
    Object getResult();


    /**
     * get attachments.
     *
     * @return attachments.
     */
    //获取绑定的额外参数
    Map<String, String> getAttachments();

    /**
     * Add the specified map to existing attachments in this instance.
     *
     * @param map
     */
    //在原有的基础上增加 这么多新的 attachment
    void addAttachments(Map<String, String> map);

    /**
     * Replace the existing attachments with the specified param.
     *
     * @param map
     */
    //替换原来的 attachment
    void setAttachments(Map<String, String> map);

    /**
     * get attachment by key.
     *
     * @return attachment value.
     */
    //获取attachment对象
    String getAttachment(String key);

    /**
     * get attachment by key with default value.
     *
     * @return attachment value.
     */
    //携带默认值的 获取attachment方法
    String getAttachment(String key, String defaultValue);

    void setAttachment(String key, String value);

}