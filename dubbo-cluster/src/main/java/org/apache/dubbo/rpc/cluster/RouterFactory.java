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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

/**
 * RouterFactory. (SPI, Singleton, ThreadSafe)
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Routing">Routing</a>
 *
 * 路由工厂对象
 * @see org.apache.dubbo.rpc.cluster.Cluster#join(Directory)
 * @see org.apache.dubbo.rpc.cluster.Directory#list(org.apache.dubbo.rpc.Invocation)
 */
@SPI
public interface RouterFactory {

    /**
     * Create router.
     *
     * 获取指定url 的 路由信息
     * @param url
     * @return router
     */
    @Adaptive("protocol")
    Router getRouter(URL url);

}