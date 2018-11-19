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
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import java.util.concurrent.Executor;

/**
 * ThreadPool
 * SPI 中的内容 代表默认拓展名是 fixed
 */
@SPI("fixed")
public interface ThreadPool {

    /**
     * Thread pool
     *
     * 通过自适应拓展类 机制 会从@Adaptive 中获取key  配置 URL 生成动态方法
     * ("url.getParameter(\"%s\", \"%s\")", key, defaultExtName)
     * 通过这行代码 获取到拓展名 再去 ExtensionLoader 获取拓展类
     * 如果 key 获取不到这里就会使用 默认的 fixed 创建对应的 线程池对象
     * @param url URL contains thread parameter
     * @return thread pool
     */
    @Adaptive({Constants.THREADPOOL_KEY})
    Executor getExecutor(URL url);

}