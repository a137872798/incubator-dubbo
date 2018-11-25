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
package org.apache.dubbo.rpc.protocol.dubbo.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.PostProcessFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;
import org.apache.dubbo.rpc.model.ConsumerModel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * EventFilter
 *
 * 过滤链
 */
//这个 设置代表了 只有消费者才能触发
@Activate(group = Constants.CONSUMER)
public class FutureFilter implements PostProcessFilter {

    protected static final Logger logger = LoggerFactory.getLogger(FutureFilter.class);

    /**
     * 正常的 调用就是从这个入口进入到 过滤链的
     * @param invoker    service
     * @param invocation invocation.
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
        //在这个方法中已经触发了 回调函数了
        fireInvokeCallback(invoker, invocation);
        // need to configure if there's return value before the invocation in order to help invoker to judge if it's
        // necessary to return future.
        return postProcessResult(invoker.invoke(invocation), invoker, invocation);
    }

    /**
     * 处理请求
     * @param result 从底层返回上来的结果对象
     * @param invoker
     * @param invocation
     * @return
     */
    @Override
    public Result postProcessResult(Result result, Invoker<?> invoker, Invocation invocation) {
        //这里 获取到的是 执行完invoker的结果对象
        if (result instanceof AsyncRpcResult) {
            AsyncRpcResult asyncResult = (AsyncRpcResult) result;
            asyncResult.thenApplyWithContext(r -> {
                //在指定时机 触发回调
                asyncCallback(invoker, invocation, r);
                return r;
            });
            return asyncResult;
        } else {
            //同步调用
            syncCallback(invoker, invocation, result);
            return result;
        }
    }

    /**
     * 处理同步 逻辑同异步一样 只是 触发的时机不一样
     * @param invoker
     * @param invocation
     * @param result
     */
    private void syncCallback(final Invoker<?> invoker, final Invocation invocation, final Result result) {
        if (result.hasException()) {
            fireThrowCallback(invoker, invocation, result.getException());
        } else {
            fireReturnCallback(invoker, invocation, result.getValue());
        }
    }

    /**
     * 处理异步回调
     * @param invoker
     * @param invocation
     * @param result
     */
    private void asyncCallback(final Invoker<?> invoker, final Invocation invocation, Result result) {
        if (result.hasException()) {
            //进入 异常链
            fireThrowCallback(invoker, invocation, result.getException());
        } else {
            //进入返回链
            fireReturnCallback(invoker, invocation, result.getValue());
        }
    }

    /**
     * 过滤链的处理逻辑
     * @param invoker
     * @param invocation
     */
    private void fireInvokeCallback(final Invoker<?> invoker, final Invocation invocation) {
        //获取消费者的 回调对象信息 里面包含了不同时机 触发的回调函数对象
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        //不存在回调对象直接返回
        if (asyncMethodInfo == null) {
            return;
        }
        //回调时 触发的方法 和 回调时 使用的实例
        final Method onInvokeMethod = asyncMethodInfo.getOninvokeMethod();
        final Object onInvokeInst = asyncMethodInfo.getOninvokeInstance();

        if (onInvokeMethod == null && onInvokeInst == null) {
            return;
        }
        if (onInvokeMethod == null || onInvokeInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a oninvoke callback config , but no such " + (onInvokeMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        //设置访问权限
        if (!onInvokeMethod.isAccessible()) {
            onInvokeMethod.setAccessible(true);
        }

        //获取参数列表
        Object[] params = invocation.getArguments();
        try {
            //反射执行 invoker时触发的回调方法
            onInvokeMethod.invoke(onInvokeInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    /**
     * 触发return时 的回调处理
     * @param invoker
     * @param invocation
     * @param result  执行的结果对象
     */
    private void fireReturnCallback(final Invoker<?> invoker, final Invocation invocation, final Object result) {
        //获取 消费者模型的回调对象执行 对应回调函数
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }

        //获取return 时 触发的回调函数 以及对应的回调对象
        final Method onReturnMethod = asyncMethodInfo.getOnreturnMethod();
        final Object onReturnInst = asyncMethodInfo.getOnreturnInstance();

        //not set onreturn callback
        if (onReturnMethod == null && onReturnInst == null) {
            return;
        }

        if (onReturnMethod == null || onReturnInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a onreturn callback config , but no such " + (onReturnMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onReturnMethod.isAccessible()) {
            onReturnMethod.setAccessible(true);
        }

        //需代理方法的 所有参数
        Object[] args = invocation.getArguments();
        Object[] params;
        //获取该方法需要的所有参数
        Class<?>[] rParaTypes = onReturnMethod.getParameterTypes();
        if (rParaTypes.length > 1) {
            //判断第二个参数是否是 可变数组对象
            if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                params = new Object[2];
                //第一个参数是结果对象 第二个是可变数组对象
                params[0] = result;
                params[1] = args;
            } else {
                //该回调 需要超过2个参数  代表不能使用 可变参数
                //就将 invocation 的参数 一个个传入
                params = new Object[args.length + 1];
                params[0] = result;
                System.arraycopy(args, 0, params, 1, args.length);
            }
        } else {
            //只能有一个参数 就是 result
            params = new Object[]{result};
        }
        try {
            //通过 需要的参数 执行回调函数
            onReturnMethod.invoke(onReturnInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    /**
     * 异常调用链
     * @param invoker
     * @param invocation
     * @param exception
     */
    private void fireThrowCallback(final Invoker<?> invoker, final Invocation invocation, final Throwable exception) {
        //获取消费者 模型对象
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }

        //获取异常时执行的 回调和 回调对象
        final Method onthrowMethod = asyncMethodInfo.getOnthrowMethod();
        final Object onthrowInst = asyncMethodInfo.getOnthrowInstance();

        //onthrow callback not configured
        if (onthrowMethod == null && onthrowInst == null) {
            return;
        }
        if (onthrowMethod == null || onthrowInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a onthrow callback config , but no such " + (onthrowMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onthrowMethod.isAccessible()) {
            onthrowMethod.setAccessible(true);
        }
        //获取该回调需要的参数  逻辑和上面一样的
        Class<?>[] rParaTypes = onthrowMethod.getParameterTypes();
        if (rParaTypes[0].isAssignableFrom(exception.getClass())) {
            try {
                Object[] args = invocation.getArguments();
                Object[] params;

                if (rParaTypes.length > 1) {
                    if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                        params = new Object[2];
                        params[0] = exception;
                        params[1] = args;
                    } else {
                        params = new Object[args.length + 1];
                        params[0] = exception;
                        System.arraycopy(args, 0, params, 1, args.length);
                    }
                } else {
                    params = new Object[]{exception};
                }
                onthrowMethod.invoke(onthrowInst, params);
            } catch (Throwable e) {
                logger.error(invocation.getMethodName() + ".call back method invoke error . callback method :" + onthrowMethod + ", url:" + invoker.getUrl(), e);
            }
        } else {
            logger.error(invocation.getMethodName() + ".call back method invoke error . callback method :" + onthrowMethod + ", url:" + invoker.getUrl(), exception);
        }
    }

    /**
     * 获取异步方法描述
     * @param invoker
     * @param invocation
     * @return
     */
    private ConsumerMethodModel.AsyncMethodInfo getAsyncMethodInfo(Invoker<?> invoker, Invocation invocation) {
        //通过服务键 获取消费模型
        final ConsumerModel consumerModel = ApplicationModel.getConsumerModel(invoker.getUrl().getServiceKey());
        if (consumerModel == null) {
            return null;
        }
        //从消费模型中寻找指定方法对象
        ConsumerMethodModel methodModel = consumerModel.getMethodModel(invocation.getMethodName());
        if (methodModel == null) {
            return null;
        }
        //获取异步信息 这个对象中包含了 各种回调方法对象
        final ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = methodModel.getAsyncInfo();
        if (asyncMethodInfo == null) {
            return null;
        }
        return asyncMethodInfo;
    }
}
