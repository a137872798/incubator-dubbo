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
package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.AsyncFor;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.RpcInvocation;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RpcUtils
 */
public class RpcUtils {

    private static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);
    private static final AtomicLong INVOKE_ID = new AtomicLong(0);

    public static Class<?> getReturnType(Invocation invocation) {
        try {
            if (invocation != null && invocation.getInvoker() != null
                    && invocation.getInvoker().getUrl() != null
                    && !invocation.getMethodName().startsWith("$")) {
                String service = invocation.getInvoker().getUrl().getServiceInterface();
                if (service != null && service.length() > 0) {
                    Class<?> invokerInterface = invocation.getInvoker().getInterface();
                    Class<?> cls = invokerInterface != null ? ReflectUtils.forName(invokerInterface.getClassLoader(), service)
                            : ReflectUtils.forName(service);
                    Method method = cls.getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                    if (method.getReturnType() == void.class) {
                        return null;
                    }
                    return method.getReturnType();
                }
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        return null;
    }

    // TODO why not get return type when initialize Invocation?

    /**
     * 根据 传入的 invocation 返回 invoker 服务方法的 返回值类型  这里看不懂 只知道返回2个 关于 返回值的 类型 一般2个值应该是相同的
     * 当出现泛型时一个是 Object 一个是 T
     * @param invocation
     * @return
     */
    public static Type[] getReturnTypes(Invocation invocation) {
        try {
            if (invocation != null && invocation.getInvoker() != null
                    && invocation.getInvoker().getUrl() != null
                    && !invocation.getMethodName().startsWith("$")) {
                //获取接口名
                String service = invocation.getInvoker().getUrl().getServiceInterface();
                if (service != null && service.length() > 0) {
                    //获取 接口类型
                    Class<?> invokerInterface = invocation.getInvoker().getInterface();
                    //获取 接口实例对象
                    Class<?> cls = invokerInterface != null ? ReflectUtils.forName(invokerInterface.getClassLoader(), service)
                            : ReflectUtils.forName(service);
                    //获取 指定方法
                    Method method = cls.getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                    if (method.getReturnType() == void.class) {
                        return null;
                    }
                    //获取返回值类型  一般情况下 2个 返回的是一样的 当出现泛型时 returnType 会变成Object genericReturnType 是 T
                    Class<?> returnType = method.getReturnType();
                    Type genericReturnType = method.getGenericReturnType();
                    //如果 返回值是 Future 的 子类 这时 genericReturnType 也应该是 Future
                    if (Future.class.isAssignableFrom(returnType)) {
                        //如果 泛化返回值类型 属于 ParameterizedType
                        if (genericReturnType instanceof ParameterizedType) {
                            //获取 参数类型
                            Type actualArgType = ((ParameterizedType) genericReturnType).getActualTypeArguments()[0];
                            if (actualArgType instanceof ParameterizedType) {
                                returnType = (Class<?>) ((ParameterizedType) actualArgType).getRawType();
                                genericReturnType = actualArgType;
                            } else {
                                returnType = (Class<?>) actualArgType;
                                genericReturnType = returnType;
                            }
                        } else {
                            returnType = null;
                            genericReturnType = null;
                        }
                    }
                    return new Type[]{returnType, genericReturnType};
                }
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        return null;
    }

    /**
     * 获取 invoker 上绑定的 id属性
     * @param inv
     * @return
     */
    public static Long getInvocationId(Invocation inv) {
        String id = inv.getAttachment(Constants.ID_KEY);
        return id == null ? null : new Long(id);
    }

    /**
     * Idempotent operation: invocation id will be added in async operation by default
     * 为 异步invocation 设置 id 属性 这里可能是要用这个id 判断 异步操作是否完成
     *
     * @param url
     * @param inv
     */
    public static void attachInvocationIdIfAsync(URL url, Invocation inv) {
        //isAttach 判断是否 是 异步的 或者是否是 auto-attach  且invocation 的 id为 null 就设置 id属性
        if (isAttachInvocationId(url, inv) && getInvocationId(inv) == null && inv instanceof RpcInvocation) {
            //设置 id 属性
            ((RpcInvocation) inv).setAttachment(Constants.ID_KEY, String.valueOf(INVOKE_ID.getAndIncrement()));
        }
    }

    /**
     * 判断是否自动关联
     * @param url
     * @param invocation
     * @return
     */
    private static boolean isAttachInvocationId(URL url, Invocation invocation) {
        //通过方法名拼接上 autoattach 获取参数
        String value = url.getMethodParameter(invocation.getMethodName(), Constants.AUTO_ATTACH_INVOCATIONID_KEY);
        if (value == null) {
            // add invocationid in async operation by default
            //判断是否是异步
            return isAsync(url, invocation);
        } else if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 从给定的invoker对象上获取方法名
     * @param invocation
     * @return
     */
    public static String getMethodName(Invocation invocation) {
        //泛化调用，第一个参数为方法名
        if (Constants.$INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 0
                && invocation.getArguments()[0] instanceof String) {
            //获取第一个参数 作为方法名
            return (String) invocation.getArguments()[0];
        }
        return invocation.getMethodName();
    }

    /**
     * 获取 invocation 方法的参数列表
     * @param invocation
     * @return
     */
    public static Object[] getArguments(Invocation invocation) {
        //泛化方法 返回第三个参数  第二个参数是参数类型
        if (Constants.$INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 2
                && invocation.getArguments()[2] instanceof Object[]) {
            return (Object[]) invocation.getArguments()[2];
        }
        return invocation.getArguments();
    }

    public static Class<?>[] getParameterTypes(Invocation invocation) {
        if (Constants.$INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 1
                && invocation.getArguments()[1] instanceof String[]) {
            String[] types = (String[]) invocation.getArguments()[1];
            if (types == null) {
                return new Class<?>[0];
            }
            Class<?>[] parameterTypes = new Class<?>[types.length];
            for (int i = 0; i < types.length; i++) {
                parameterTypes[i] = ReflectUtils.forName(types[0]);
            }
            return parameterTypes;
        }
        return invocation.getParameterTypes();
    }

    /**
     * 判断是否是异步的
     * @param url
     * @param inv
     * @return
     */
    public static boolean isAsync(URL url, Invocation inv) {
        boolean isAsync;
        //通过 获取invoker 上的 标识 判断是否是 异步
        if (Boolean.TRUE.toString().equals(inv.getAttachment(Constants.ASYNC_KEY))) {
            isAsync = true;
        } else {
            //一级级 查找对应属性
            isAsync = url.getMethodParameter(getMethodName(inv), Constants.ASYNC_KEY, false);
        }
        return isAsync;
    }

    public static boolean isGeneratedFuture(Invocation inv) {
        return Boolean.TRUE.toString().equals(inv.getAttachment(Constants.FUTURE_GENERATED_KEY));
    }

    /**
     * 判断 该方法属于的类上有没有 AsyncFor 注解 且方法名 以async 结尾 且 返回值 实现了 CompletableFuture接口
     * @param method
     * @return
     */
    public static boolean hasGeneratedFuture(Method method) {
        Class<?> clazz = method.getDeclaringClass();
        return clazz.isAnnotationPresent(AsyncFor.class) && method.getName().endsWith(Constants.ASYNC_SUFFIX) && hasFutureReturnType(method);
    }

    public static boolean isFutureReturnType(Invocation inv) {
        return Boolean.TRUE.toString().equals(inv.getAttachment(Constants.FUTURE_RETURNTYPE_KEY));
    }

    public static boolean hasFutureReturnType(Method method) {
        return CompletableFuture.class.isAssignableFrom(method.getReturnType());
    }

    public static boolean isOneway(URL url, Invocation inv) {
        boolean isOneway;
        if (Boolean.FALSE.toString().equals(inv.getAttachment(Constants.RETURN_KEY))) {
            isOneway = true;
        } else {
            isOneway = !url.getMethodParameter(getMethodName(inv), Constants.RETURN_KEY, true);
        }
        return isOneway;
    }

    /**
     * 获取必须的 attachment  (移除异步信息)
     */
    public static Map<String, String> getNecessaryAttachments(Invocation inv) {
        //获取 inv 的attach 容器
        Map<String, String> attachments = new HashMap<>(inv.getAttachments());
        //移除异步相关的 属性后返回
        attachments.remove(Constants.ASYNC_KEY);
        attachments.remove(Constants.FUTURE_GENERATED_KEY);
        return attachments;
    }

}
