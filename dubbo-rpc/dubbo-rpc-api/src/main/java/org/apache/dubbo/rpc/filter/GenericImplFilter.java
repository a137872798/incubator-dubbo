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
 * 泛化调用链 只针对消费者  并且该消费者必须要有 generic 的标识 才能起作用
 */
//只针对消费者
@Activate(group = Constants.CONSUMER, value = Constants.GENERIC_KEY, order = 20000)
public class GenericImplFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(GenericImplFilter.class);

    /**
     * generic 的 参数类型 第一个是方法名 第二个是参数列表名 第三个是参数列表
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
        //如果是 非泛化形式 但是 设置了  generic = true 那么就在这里 转换为 泛化形式 也就是genericService.$invoke(...)
        if (ProtocolUtils.isGeneric(generic)
                //这里好像是避免被重复处理
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
                //这里 只有generic= true了  没有看到关于 nativejava的 相关代码
            } else {
                args = PojoUtils.generalize(arguments);
            }

            //设置方法名
            invocation2.setMethodName(Constants.$INVOKE);
            invocation2.setParameterTypes(GENERIC_PARAMETER_TYPES);
            //将原来的 方法名作为参数设置
            invocation2.setArguments(new Object[]{methodName, types, args});
            //将参数进行转换后开始一般调用
            Result result = invoker.invoke(invocation2);

            //返回结果无异常时
            if (!result.hasException()) {
                Object value = result.getValue();
                try {
                    //通过指定方法名和参数 获取method对象
                    Method method = invoker.getInterface().getMethod(methodName, parameterTypes);
                    //如果是 bean的 序列化方式
                    if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                        if (value == null) {
                            //如果没有结果 就不需要反序列化
                            return new RpcResult(value);
                        } else if (value instanceof JavaBeanDescriptor) {
                            //将反序列化的结果设置到result中
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
                        //这里 默认就是 generic=true 时进行 的反序列化
                        return new RpcResult(PojoUtils.realize(value, method.getReturnType(), method.getGenericReturnType()));
                    }
                } catch (NoSuchMethodException e) {
                    throw new RpcException(e.getMessage(), e);
                }
                //如果抛出的异常 是 generic 类型的
            } else if (result.getException() instanceof GenericException) {
                GenericException exception = (GenericException) result.getException();
                try {
                    //获取 封装的 异常类名
                    String className = exception.getExceptionClass();
                    //创建异常对象
                    Class<?> clazz = ReflectUtils.forName(className);
                    Throwable targetException = null;
                    Throwable lastException = null;
                    try {
                        targetException = (Throwable) clazz.newInstance();
                    } catch (Throwable e) {
                        //如果又抛出了异常 通过所有该类的 构造方式创建异常对象
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
                    //创建成功时
                    if (targetException != null) {
                        try {
                            //获取异常的 详细 消息信息
                            Field field = Throwable.class.getDeclaredField("detailMessage");
                            if (!field.isAccessible()) {
                                field.setAccessible(true);
                            }
                            //还原 该异常对象 从包装的  genericException 中又拿出异常消息
                            field.set(targetException, exception.getExceptionMessage());
                        } catch (Throwable e) {
                            logger.warn(e.getMessage(), e);
                        }
                        result = new RpcResult(targetException);
                        //创建失败且 lastException 不为空 就直接抛出
                    } else if (lastException != null) {
                        throw lastException;
                    }
                } catch (Throwable e) {
                    throw new RpcException("Can not deserialize exception " + exception.getExceptionClass() + ", message: " + exception.getExceptionMessage(), e);
                }
            }
            return result;
        }

        //如果直接是以泛化形式进行调用的 比如
        /**
         * GenericService genericService = (GenericService) context.getBean("demoService");
         * Object result = genericService.$invoke("say01", new String[]{"java.lang.String"}, new Object[]{"123"});
         */
        if (invocation.getMethodName().equals(Constants.$INVOKE)
                && invocation.getArguments() != null
                && invocation.getArguments().length == 3
                && ProtocolUtils.isGeneric(generic)) {

            Object[] args = (Object[]) invocation.getArguments()[2];
            //如果是 nativejava 的序列化方式
            if (ProtocolUtils.isJavaGenericSerialization(generic)) {

                //一旦 参数 不是 byte[]类型
                for (Object arg : args) {
                    if (!(byte[].class == arg.getClass())) {
                        error(generic, byte[].class.getName(), arg.getClass().getName());
                    }
                }
                //如果是bean 的序列化方式
            } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                //这里也做了参数校验
                for (Object arg : args) {
                    if (!(arg instanceof JavaBeanDescriptor)) {
                        error(generic, JavaBeanDescriptor.class.getName(), arg.getClass().getName());
                    }
                }
            }

            //将参数存入 attachment
            ((RpcInvocation) invocation).setAttachment(
                    Constants.GENERIC_KEY, invoker.getUrl().getParameter(Constants.GENERIC_KEY));
        }
        //已经泛化好就直接进这里
        return invoker.invoke(invocation);
    }

    /**
     * 将异常信息封装成 异常对象
     * @param generic
     * @param expected
     * @param actual
     * @throws RpcException
     */
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
