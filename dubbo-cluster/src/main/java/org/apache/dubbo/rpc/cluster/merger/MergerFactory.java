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

package org.apache.dubbo.rpc.cluster.merger;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.cluster.Merger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * merger 工厂对象
 */
public class MergerFactory {

    /**
     * 根据 融合的类型 关联对应的 融合类
     */
    private static final ConcurrentMap<Class<?>, Merger<?>> mergerCache =
            new ConcurrentHashMap<Class<?>, Merger<?>>();

    public static <T> Merger<T> getMerger(Class<T> returnType) {
        Merger result;
        //如果需要的返回类型是 数组
        if (returnType.isArray()) {
            //先获取 数组内的元素对象
            Class type = returnType.getComponentType();
            //通过 缓存对象获取 对应的 merger对象
            result = mergerCache.get(type);
            if (result == null) {
                //加载merger对象
                loadMergers();
                result = mergerCache.get(type);
            }
            if (result == null && !type.isPrimitive()) {
                //返回一个 Object merger对象 可以强转为需要的类型
                result = ArrayMerger.INSTANCE;
            }
        } else {
            //普通类型 直接查询
            result = mergerCache.get(returnType);
            if (result == null) {
                loadMergers();
                result = mergerCache.get(returnType);
            }
        }
        return result;
    }

    /**
     * 加载 merger对象
     */
    static void loadMergers() {
        //获取所有支持的 拓展类型  加载SPI 文件中设置的 所有Merger 的实现类
        Set<String> names = ExtensionLoader.getExtensionLoader(Merger.class)
                .getSupportedExtensions();
        for (String name : names) {
            //获取对应的merger对象
            Merger m = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(name);
            //将m 转换为 泛化类型 作为 key
            mergerCache.putIfAbsent(ReflectUtils.getGenericClass(m.getClass()), m);
        }
    }

}
