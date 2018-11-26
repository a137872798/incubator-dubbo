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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.io.UnsafeByteArrayOutputStream;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * GenericInvokerFilter.
 * 泛化过滤链
 */
//仅限服务提供者
@Activate(group = Constants.PROVIDER, order = -20000)
public class GenericFilter implements Filter {

    /**
     * 进行泛化过滤
     * @param invoker    service
     * @param inv
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        //如果没有实现泛化接口
        if (inv.getMethodName().equals(Constants.$INVOKE)
                && inv.getArguments() != null
                && inv.getArguments().length == 3
                && !GenericService.class.isAssignableFrom(invoker.getInterface())) {
            //3个参数分别是方法名 参数类型列表 参数列表
            String name = ((String) inv.getArguments()[0]).trim();
            //这里的 string 记录的 是类型的全限定名
            String[] types = (String[]) inv.getArguments()[1];
            Object[] args = (Object[]) inv.getArguments()[2];
            try {
                //通过接口和 类型 查找对应的方法
                Method method = ReflectUtils.findMethodByMethodSignature(invoker.getInterface(), name, types);
                //获取该方法的参数列表
                Class<?>[] params = method.getParameterTypes();
                if (args == null) {
                    //该方法没有参数 就将args滞空
                    args = new Object[params.length];
                }
                //获取 泛化信息
                String generic = inv.getAttachment(Constants.GENERIC_KEY);

                //从inv 上没有获取到就要从全局上下文中找
                if (StringUtils.isBlank(generic)) {
                    generic = RpcContext.getContext().getAttachment(Constants.GENERIC_KEY);
                }

                //如果 没有泛化信息 或者是默认的泛化信息 也就是generic = true
                if (StringUtils.isEmpty(generic)
                        || ProtocolUtils.isDefaultGenericSerialization(generic)) {
                    //参数列表中如果有泛型 会返回泛型信息
                    args = PojoUtils.realize(args, params, method.getGenericParameterTypes());
                    //如果是 generic = nativejava
                } else if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                    for (int i = 0; i < args.length; i++) {
                        //找到参数中的byte[]
                        if (byte[].class == args[i].getClass()) {
                            try {
                                //通过SPI找到指定的序列化方式 进行解析并返回结果
                                UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream((byte[]) args[i]);
                                args[i] = ExtensionLoader.getExtensionLoader(Serialization.class)
                                        .getExtension(Constants.GENERIC_SERIALIZATION_NATIVE_JAVA)
                                        .deserialize(null, is).readObject();
                            } catch (Exception e) {
                                throw new RpcException("Deserialize argument [" + (i + 1) + "] failed.", e);
                            }
                        } else {
                            //nativejava 应该所有参数的都是byte[]
                            throw new RpcException(
                                    "Generic serialization [" +
                                            Constants.GENERIC_SERIALIZATION_NATIVE_JAVA +
                                            "] only support message type " +
                                            byte[].class +
                                            " and your message type is " +
                                            args[i].getClass());
                        }
                    }
                    //序列化方式是否是bean
                } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                    //这里参数也必须都是 bean类型
                    for (int i = 0; i < args.length; i++) {
                        if (args[i] instanceof JavaBeanDescriptor) {
                            args[i] = JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) args[i]);
                        } else {
                            throw new RpcException(
                                    "Generic serialization [" +
                                            Constants.GENERIC_SERIALIZATION_BEAN +
                                            "] only support message type " +
                                            JavaBeanDescriptor.class.getName() +
                                            " and your message type is " +
                                            args[i].getClass().getName());
                        }
                    }
                }
                //将改造后的 参数 传入 之后的操作都跟原来一样 上面就是对传入参数做了处理
                Result result = invoker.invoke(new RpcInvocation(method, args, inv.getAttachments()));

                //下面针对结果 又进行了序列化 保持跟传入时 的格式一致 因为装饰器模式使得调用起来像是aop 每个单元都可以为其他单元的
                //调用前后做处理

                //这里将 异常包装成了 GenericException
                if (result.hasException()
                        && !(result.getException() instanceof GenericException)) {
                    return new RpcResult(new GenericException(result.getException()));
                }
                //如果是 nativejava
                if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                    try {
                        //将结果重新序列化
                        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream(512);
                        ExtensionLoader.getExtensionLoader(Serialization.class)
                                .getExtension(Constants.GENERIC_SERIALIZATION_NATIVE_JAVA)
                                .serialize(null, os).writeObject(result.getValue());
                        return new RpcResult(os.toByteArray());
                    } catch (IOException e) {
                        throw new RpcException("Serialize result failed.", e);
                    }
                    //如果是bean 也要重新序列化
                } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                    return new RpcResult(JavaBeanSerializeUtil.serialize(result.getValue(), JavaBeanAccessor.METHOD));
                } else {
                    //这里代表是 默认的 generic 即 generic = true
                    return new RpcResult(PojoUtils.generalize(result.getValue()));
                }
            } catch (NoSuchMethodException e) {
                throw new RpcException(e.getMessage(), e);
            } catch (ClassNotFoundException e) {
                throw new RpcException(e.getMessage(), e);
            }
        }
        //如果一开始不满足泛化添加 就直接委托到调用链下一层
        return invoker.invoke(inv);
    }

}
