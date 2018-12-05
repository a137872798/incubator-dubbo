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
 * 泛化调用链 只针对消费者  并且该消费者必须要有 generic 的标识 才能起作用 先从泛化调用开始 在这里做处理后 才会进入 泛化引用
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
     * @param invoker    service 链中的 上层元素
     * @param invocation invocation. 调用invoke 传入的上下文对象记录了 方法名等信息
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //获取 generic 的 值 可能是 nativejava true bean
        String generic = invoker.getUrl().getParameter(Constants.GENERIC_KEY);
        //确定是否是 泛化类型
        if (ProtocolUtils.isGeneric(generic)
                //如果已经处理过的 方法名会变成 $invoke 这里是避免重复处理
                && !Constants.$INVOKE.equals(invocation.getMethodName())
                //invocation 一定要是 RPCinvocation???
                && invocation instanceof RpcInvocation) {
            RpcInvocation invocation2 = (RpcInvocation) invocation;
            //转型获取方法名  这里的方法名还没有被处理过 处理后这个methodName 需要是 $invoke
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
                    //生成对应参数的 描述信息  JavaBeanAccessor.METHOD 代表描述范围
                    args[i] = JavaBeanSerializeUtil.serialize(arguments[i], JavaBeanAccessor.METHOD);
                }
                //这里 只有generic= true了  没有看到关于 nativejava的 相关代码
            } else {
                //也是 获取参数信息
                args = PojoUtils.generalize(arguments);
            }

            //设置方法名
            invocation2.setMethodName(Constants.$INVOKE);
            //设置成 泛化对象的 标准参数
            invocation2.setParameterTypes(GENERIC_PARAMETER_TYPES);
            //对应 GENERIC_PARAMETER_TYPES 需要的参数
            invocation2.setArguments(new Object[]{methodName, types, args});
            //使用泛化发起 调用
            Result result = invoker.invoke(invocation2);

            //返回结果无异常时
            if (!result.hasException()) {
                Object value = result.getValue();
                try {
                    //这里要将 泛化返回的 结果 也就是 map类型转换成 本身需要的 结果

                    //通过指定方法名和参数 获取method对象  getInterface() 也就是返回一开始消费者需要的 服务接口类型
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
                            //将异常信息 转移到这里
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

        //如果该invoker 对象一开始就已经泛化好了  这里就是做校验工作
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
