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

/**
 *
 */
public abstract class AbstractPostProcessFilter implements PostProcessFilter {

    /**
     * 处理下层传过来的 请求
     * @param result invoke链返回的结果对象
     * @param invoker 发起该请求的那层invoker
     * @param invocation
     * @return
     */
    @Override
    public Result postProcessResult(Result result, Invoker<?> invoker, Invocation invocation) {
        //如果是 异步结果
        if (result instanceof AsyncRpcResult) {
            AsyncRpcResult asyncResult = (AsyncRpcResult) result;
            //在计算结束后调用doPost
            asyncResult.thenApplyWithContext(r -> doPostProcess(r, invoker, invocation));
            return asyncResult;
        } else {
            //同步就直接调用doPost
            return doPostProcess(result, invoker, invocation);
        }
    }

    protected abstract Result doPostProcess(Result result, Invoker<?> invoker, Invocation invocation);
}
