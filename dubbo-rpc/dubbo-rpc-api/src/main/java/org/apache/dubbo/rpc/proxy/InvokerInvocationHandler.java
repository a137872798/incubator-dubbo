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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 *
 * invoker 的 处理器对象 这个 就是实现了 jdk 的那个接口
 */
public class InvokerInvocationHandler implements InvocationHandler {

    /**
     * 处理器对象
     */
    private final Invoker<?> invoker;

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
    }

    /**
     * 调用方法
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        //获取 方法名和 该方法需要的参数
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();

        //这里执行的 都是 invoker 的 方法 而不是proxy的

        //如果 是 Object 的方法  传入的 执行对象是 invoker
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }
        //这3个 方法 不再通过invoke 进行调用 而是 直接 使用invoker对象本身的 方法  一般就是object的方法
        if ("toString".equals(methodName) && parameterTypes.length == 0) {
            return invoker.toString();
        }
        if ("hashCode".equals(methodName) && parameterTypes.length == 0) {
            return invoker.hashCode();
        }
        if ("equals".equals(methodName) && parameterTypes.length == 1) {
            return invoker.equals(args[0]);
        }

        //将生成 invocation 的动作放到了 这里 这样就对外隐藏
        //创建 RPC 调用对象
        RpcInvocation invocation;
        //是否是异步  这段看不懂 先不管
        if (RpcUtils.hasGeneratedFuture(method)) {
            Class<?> clazz = method.getDeclaringClass();
            //获取同步方法的长度 因为异步方法在最后会加 异步标识
            String syncMethodName = methodName.substring(0, methodName.length() - Constants.ASYNC_SUFFIX.length());
            //获取同步方法版本
            Method syncMethod = clazz.getMethod(syncMethodName, method.getParameterTypes());
            //创建 RPC invoker 对象
            invocation = new RpcInvocation(syncMethod, args);
            //携带代表是 future 和 async 的标识
            invocation.setAttachment(Constants.FUTURE_GENERATED_KEY, "true");
            invocation.setAttachment(Constants.ASYNC_KEY, "true");
        } else {
            //创建 RPC invoker 对象
            invocation = new RpcInvocation(method, args);
            //返回值是否 实现了 CompletableFuture 接口 是就 携带 async 和 future 标识
            if (RpcUtils.hasFutureReturnType(method)) {
                invocation.setAttachment(Constants.FUTURE_RETURNTYPE_KEY, "true");
                invocation.setAttachment(Constants.ASYNC_KEY, "true");
            }
        }
        //这个 动态代理没有使用传入的 proxy 而是全程调用 初始化的 invoker 对象
        return invoker.invoke(invocation).recreate();
    }


}
