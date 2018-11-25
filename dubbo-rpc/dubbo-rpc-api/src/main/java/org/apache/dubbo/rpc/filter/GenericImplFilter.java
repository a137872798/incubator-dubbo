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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.beanutil.JavaBeanAccessor;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * GenericImplInvokerFilter
 *
 * 泛化调用链
 */
//只针对消费者
@Activate(group = Constants.CONSUMER, value = Constants.GENERIC_KEY, order = 20000)
public class GenericImplFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(GenericImplFilter.class);

    /**
     * generic 的 参数类型
     */
    private static final Class<?>[] GENERIC_PARAMETER_TYPES = new Class<?>[]{String.class, String[].class, Object[].class};

    /**
     * 调用链的实际逻辑
     * @param invoker    service
     * @param invocation invocation.
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //获取 generic 的 值 可能是 nativejava true bean
        String generic = invoker.getUrl().getParameter(Constants.GENERIC_KEY);
        if (ProtocolUtils.isGeneric(generic)
                && !Constants.$INVOKE.equals(invocation.getMethodName())
                //invocation 一定要是 RPCinvocation???
                && invocation instanceof RpcInvocation) {
            RpcInvocation invocation2 = (RpcInvocation) invocation;
            //转型获取方法名
            String methodName = invocation2.getMethodName();
            //参数列表和参数类型
            Class<?>[] parameterTypes = invocation2.getParameterTypes();
            Object[] arguments = invocation2.getArguments();

            //存放参数类型名
            String[] types = new String[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                //java.lang.Object[][].class => "java.lang.Object[][]" 处理数组名
                types[i] = ReflectUtils.getName(parameterTypes[i]);
            }

            Object[] args;
            //如果是bean 的序列化方式
            if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                //设置参数列表
                args = new Object[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    //每个参数都进行序列化
                    args[i] = JavaBeanSerializeUtil.serialize(arguments[i], JavaBeanAccessor.METHOD);
                }
            } else {
                args = PojoUtils.generalize(arguments);
            }

            invocation2.setMethodName(Constants.$INVOKE);
            invocation2.setParameterTypes(GENERIC_PARAMETER_TYPES);
            invocation2.setArguments(new Object[]{methodName, types, args});
            Result result = invoker.invoke(invocation2);

            if (!result.hasException()) {
                Object value = result.getValue();
                try {
                    Method method = invoker.getInterface().getMethod(methodName, parameterTypes);
                    if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                        if (value == null) {
                            return new RpcResult(value);
                        } else if (value instanceof JavaBeanDescriptor) {
                            return new RpcResult(JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) value));
                        } else {
                            throw new RpcException(
                                    "The type of result value is " +
                                            value.getClass().getName() +
                                            " other than " +
                                            JavaBeanDescriptor.class.getName() +
                                            ", and the result is " +
                                            value);
                        }
                    } else {
                        return new RpcResult(PojoUtils.realize(value, method.getReturnType(), method.getGenericReturnType()));
                    }
                } catch (NoSuchMethodException e) {
                    throw new RpcException(e.getMessage(), e);
                }
            } else if (result.getException() instanceof GenericException) {
                GenericException exception = (GenericException) result.getException();
                try {
                    String className = exception.getExceptionClass();
                    Class<?> clazz = ReflectUtils.forName(className);
                    Throwable targetException = null;
                    Throwable lastException = null;
                    try {
                        targetException = (Throwable) clazz.newInstance();
                    } catch (Throwable e) {
                        lastException = e;
                        for (Constructor<?> constructor : clazz.getConstructors()) {
                            try {
                                targetException = (Throwable) constructor.newInstance(new Object[constructor.getParameterTypes().length]);
                                break;
                            } catch (Throwable e1) {
                                lastException = e1;
                            }
                        }
                    }
                    if (targetException != null) {
                        try {
                            Field field = Throwable.class.getDeclaredField("detailMessage");
                            if (!field.isAccessible()) {
                                field.setAccessible(true);
                            }
                            field.set(targetException, exception.getExceptionMessage());
                        } catch (Throwable e) {
                            logger.warn(e.getMessage(), e);
                        }
                        result = new RpcResult(targetException);
                    } else if (lastException != null) {
                        throw lastException;
                    }
                } catch (Throwable e) {
                    throw new RpcException("Can not deserialize exception " + exception.getExceptionClass() + ", message: " + exception.getExceptionMessage(), e);
                }
            }
            return result;
        }

        if (invocation.getMethodName().equals(Constants.$INVOKE)
                && invocation.getArguments() != null
                && invocation.getArguments().length == 3
                && ProtocolUtils.isGeneric(generic)) {

            Object[] args = (Object[]) invocation.getArguments()[2];
            if (ProtocolUtils.isJavaGenericSerialization(generic)) {

                for (Object arg : args) {
                    if (!(byte[].class == arg.getClass())) {
                        error(generic, byte[].class.getName(), arg.getClass().getName());
                    }
                }
            } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                for (Object arg : args) {
                    if (!(arg instanceof JavaBeanDescriptor)) {
                        error(generic, JavaBeanDescriptor.class.getName(), arg.getClass().getName());
                    }
                }
            }

            ((RpcInvocation) invocation).setAttachment(
                    Constants.GENERIC_KEY, invoker.getUrl().getParameter(Constants.GENERIC_KEY));
        }
        return invoker.invoke(invocation);
    }

    private void error(String generic, String expected, String actual) throws RpcException {
        throw new RpcException(
                "Generic serialization [" +
                        generic +
                        "] only support message type " +
                        expected +
                        " and your message type is " +
                        actual);
    }

}
