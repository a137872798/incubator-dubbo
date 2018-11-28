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
package org.apache.dubbo.rpc.cluster.router.file;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.IOUtils;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.router.script.ScriptRouterFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * 文件路由工厂
 */
public class FileRouterFactory implements RouterFactory {

    public static final String NAME = "file";

    /**
     * 路由工厂对象 也就是 文件路由工厂并不能直接创建 路由对象
     */
    private RouterFactory routerFactory;

    public void setRouterFactory(RouterFactory routerFactory) {
        this.routerFactory = routerFactory;
    }

    /**
     * 获取路由对象
     * @param url
     * @return
     */
    @Override
    public Router getRouter(URL url) {
        try {
            // Transform File URL into Script Route URL, and Load
            // file:///d:/path/to/route.js?router=script ==> script:///d:/path/to/route.js?type=js&rule=<file-content>
            //获取 url 中关于路由的信息
            String protocol = url.getParameter(Constants.ROUTER_KEY, ScriptRouterFactory.NAME); // Replace original protocol (maybe 'file') with 'script'
            String type = null; // Use file suffix to config script type, e.g., js, groovy ...
            //获取 路径 path 好像是一个文件路径 可能是每个url 都有一个对应的文件路径 用来存放相关数据
            String path = url.getPath();
            if (path != null) {
                int i = path.lastIndexOf('.');
                if (i > 0) {
                    //截取最后一段数据 这个是该文件的 类型 也就是脚本类型
                    type = path.substring(i + 1);
                }
            }
            //创建文件输入流并读取数据
            String rule = IOUtils.read(new FileReader(new File(url.getAbsolutePath())));

            //判断是否是 runtime 这个标识是???
            boolean runtime = url.getParameter(Constants.RUNTIME_KEY, false);
            //为url 设置 协议和 type 角色 runtime
            URL script = url.setProtocol(protocol).addParameter(Constants.TYPE_KEY, type).addParameter(Constants.RUNTIME_KEY, runtime).addParameterAndEncoded(Constants.RULE_KEY, rule);

            //将 修改过的 url 传入
            return routerFactory.getRouter(script);
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
