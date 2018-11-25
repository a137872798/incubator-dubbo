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

package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 编解码 支持对象
 */
public class CodecSupport {

    private static final Logger logger = LoggerFactory.getLogger(CodecSupport.class);
    /**
     * 序列化 对象集合
     * key：序列化类型编号 {@link Serialization#getContentTypeId()}
     */
    private static Map<Byte, Serialization> ID_SERIALIZATION_MAP = new HashMap<Byte, Serialization>();
    /**
     * 序列化名集合
     * key：序列化类型编号 {@link Serialization#getContentTypeId()}
     */
    private static Map<Byte, String> ID_SERIALIZATIONNAME_MAP = new HashMap<Byte, String>();

    static {
        //获取 所有 拓展对象
        Set<String> supportedExtensions = ExtensionLoader.getExtensionLoader(Serialization.class).getSupportedExtensions();
        //遍历拓展对象
        for (String name : supportedExtensions) {
            //获取对应的拓展对象
            Serialization serialization = ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(name);
            //获取 id 并保存到容器中
            byte idByte = serialization.getContentTypeId();
            //重复就 跳过
            if (ID_SERIALIZATION_MAP.containsKey(idByte)) {
                logger.error("Serialization extension " + serialization.getClass().getName()
                        + " has duplicate id to Serialization extension "
                        + ID_SERIALIZATION_MAP.get(idByte).getClass().getName()
                        + ", ignore this Serialization extension");
                continue;
            }
            ID_SERIALIZATION_MAP.put(idByte, serialization);
            ID_SERIALIZATIONNAME_MAP.put(idByte, name);
        }
    }

    private CodecSupport() {
    }

    /**
     * 通过 id 获取序列化方式
     * @param id
     * @return
     */
    public static Serialization getSerializationById(Byte id) {
        return ID_SERIALIZATION_MAP.get(id);
    }

    /**
     * 通过url 中的参数 获取序列化方式 默认使用 hessian2
     * @param url
     * @return
     */
    public static Serialization getSerialization(URL url) {
        return ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(
                url.getParameter(Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION));
    }

    /**
     * 通过 url 和 id 获取序列化方式 在方法内部 进行校验
     */
    public static Serialization getSerialization(URL url, Byte id) throws IOException {
        Serialization serialization = getSerializationById(id);
        String serializationName = url.getParameter(Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION);
        // Check if "serialization id" passed from network matches the id on this side(only take effect for JDK serialization), for security purpose.
        if (serialization == null
                || ((id == 3 || id == 7 || id == 4) && !(serializationName.equals(ID_SERIALIZATIONNAME_MAP.get(id))))) {
            throw new IOException("Unexpected serialization id:" + id + " received from network, please check if the peer send the right id.");
        }
        return serialization;
    }

    /**
     * 反序列化
     * @param url
     * @param is
     * @param proto
     * @return
     * @throws IOException
     */
    public static ObjectInput deserialize(URL url, InputStream is, byte proto) throws IOException {
        //这里顺带进行了校验 url 中的 序列化方式 与 proto 是否相符合
        Serialization s = getSerialization(url, proto);
        //反序列化  返回一个 dubbo 的对象流
        return s.deserialize(url, is);
    }
}
