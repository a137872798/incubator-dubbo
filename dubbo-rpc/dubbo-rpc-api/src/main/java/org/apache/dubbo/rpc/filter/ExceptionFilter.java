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
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.AbstractPostProcessFilter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.service.GenericService;

import java.lang.reflect.Method;


/**
 * ExceptionInvokerFilter
 * <p>
 * Functions:
 * <ol>
 * <li>unexpected exception will be logged in ERROR level on provider side. Unexpected exception are unchecked
 * exception not declared on the interface</li>
 * <li>Wrap the exception not introduced in API package into RuntimeException. Framework will serialize the outer exception but stringnize its cause in order to avoid of possible serialization problem on client side</li>
 * </ol>
 *
 * 针对服务提供者的 处理一些 预期外的异常对象 所以是非受检异常 因为受检异常在一开始都必须要声明出来
 */
@Activate(group = Constants.PROVIDER)
public class ExceptionFilter extends AbstractPostProcessFilter {

    private final Logger logger;

    public ExceptionFilter() {
        this(LoggerFactory.getLogger(ExceptionFilter.class));
    }

    public ExceptionFilter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            //处理返回的结果
            return postProcessResult(invoker.invoke(invocation), invoker, invocation);
        } catch (RuntimeException e) {
            logger.error("Got unchecked and undeclared exception which called by " + RpcContext.getContext().getRemoteHost()
                    + ". service: " + invoker.getInterface().getName() + ", method: " + invocation.getMethodName()
                    + ", exception: " + e.getClass().getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 处理 invoker 之后的 结果  只处理 非受检异常 且 没有出现在方法的 签名中
     * @param result
     * @param invoker
     * @param invocation
     * @return
     */
    @Override
    protected Result doPostProcess(Result result, Invoker<?> invoker, Invocation invocation) {
        //如果调用结果有异常 且 没有实现泛化接口
        if (result.hasException() && GenericService.class != invoker.getInterface()) {
            try {
                //获取异常对象
                Throwable exception = result.getException();

                // directly throw if it's checked exception
                // 如果是 受检异常 直接抛出
                if (!(exception instanceof RuntimeException) && (exception instanceof Exception)) {
                    return result;
                }
                // directly throw if the exception appears in the signature
                try {
                    //通过方法名和 参数列表 定位到具体的方法
                    Method method = invoker.getInterface().getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                    //获取该方法可能抛出的异常列表
                    Class<?>[] exceptionClassses = method.getExceptionTypes();
                    for (Class<?> exceptionClass : exceptionClassses) {
                        //找到对应的异常类 直接返回
                        if (exception.getClass().equals(exceptionClass)) {
                            return result;
                        }
                    }
                    //找不到对应方法就返回
                } catch (NoSuchMethodException e) {
                    return result;
                }

                // for the exception not found in method's signature, print ERROR message in server's log.
                // 没有从方法签名中找到 对应的异常  且该异常是 非受检异常
                logger.error("Got unchecked and undeclared exception which called by " + RpcContext.getContext().getRemoteHost()
                        + ". service: " + invoker.getInterface().getName() + ", method: " + invocation.getMethodName()
                        + ", exception: " + exception.getClass().getName() + ": " + exception.getMessage(), exception);

                // directly throw if exception class and interface class are in the same jar file.
                // 接口和 类在一个 jar包中 也抛出???
                // 获取类 和 接口的 文件名
                String serviceFile = ReflectUtils.getCodeBase(invoker.getInterface());
                String exceptionFile = ReflectUtils.getCodeBase(exception.getClass());
                if (serviceFile == null || exceptionFile == null || serviceFile.equals(exceptionFile)) {
                    return result;
                }
                // directly throw if it's JDK exception
                // 如果是jdk 的异常也直接返回
                String className = exception.getClass().getName();
                if (className.startsWith("java.") || className.startsWith("javax.")) {
                    return result;
                }
                // directly throw if it's dubbo exception
                // dubbo 异常也返回
                if (exception instanceof RpcException) {
                    return result;
                }

                // otherwise, wrap with RuntimeException and throw back to the client
                // 将该异常封装成一个 普通的 runtimeException
                return new RpcResult(new RuntimeException(StringUtils.toString(exception)));
            } catch (Throwable e) {
                logger.warn("Fail to ExceptionFilter when called by " + RpcContext.getContext().getRemoteHost()
                        + ". service: " + invoker.getInterface().getName() + ", method: " + invocation.getMethodName()
                        + ", exception: " + e.getClass().getName() + ": " + e.getMessage(), e);
                return result;
            }
        }
        return result;
    }

}

