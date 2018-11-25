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
package org.apache.dubbo.rpc.service;

/**
 * Generic service interface
 *
 * 泛化服务接口  泛化接口就是客户端没有确定 api接口 和 模型类元 参数和返回值都是map
 * 一个泛化引用只对应一个服务实现
 * generic 的 3种值  true bean nativejava 代表的是 从 pojo 到一般化(map) 的序列化方式 一旦判定是泛型类 在初始化过程中 serviceInterface 就会被替换成这个
 * @export
 */
public interface GenericService {

    /**
     * Generic invocation
     *
     * 泛化调用
     * @param method         Method name, e.g. findPerson. If there are overridden methods, parameter info is
     *                       required, e.g. findPerson(java.lang.String)
     * @param parameterTypes Parameter types
     * @param args           Arguments
     * @return invocation return value
     * @throws Throwable potential exception thrown from the invocation
     */
    Object $invoke(String method, String[] parameterTypes, Object[] args) throws GenericException;

}