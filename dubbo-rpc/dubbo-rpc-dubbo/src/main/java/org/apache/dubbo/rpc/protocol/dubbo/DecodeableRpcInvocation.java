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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Decodeable;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.RpcInvocation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.decodeInvocationArgument;

/**
 * dubbo 协议独有对象
 */
public class DecodeableRpcInvocation extends RpcInvocation implements Codec, Decodeable {

    private static final Logger log = LoggerFactory.getLogger(DecodeableRpcInvocation.class);

    /**
     * dubbo 的通道对象
     */
    private Channel channel;

    /**
     * 序列化类型
     */
    private byte serializationType;

    /**
     * 输入流
     */
    private InputStream inputStream;

    /**
     * 请求对象
     */
    private Request request;

    private volatile boolean hasDecoded;

    public DecodeableRpcInvocation(Channel channel, Request request, InputStream is, byte id) {
        Assert.notNull(channel, "channel == null");
        Assert.notNull(request, "request == null");
        Assert.notNull(is, "inputStream == null");
        this.channel = channel;
        this.request = request;
        this.inputStream = is;
        this.serializationType = id;
    }

    /**
     * 解码 使用了 channel 和 输入流
     * @throws Exception
     */
    @Override
    public void decode() throws Exception {
        if (!hasDecoded && channel != null && inputStream != null) {
            try {
                decode(channel, inputStream);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode rpc invocation failed: " + e.getMessage(), e);
                }
                //broken 代表该请求失败了  当解析失败时 会设置这个数据 给请求发起方
                request.setBroken(true);
                request.setData(e);
            } finally {
                hasDecoded = true;
            }
        }
    }

    @Override
    public void encode(Channel channel, OutputStream output, Object message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * 解码的 核心逻辑
     * @param channel channel.
     * @param input   input stream.
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, InputStream input) throws IOException {
        //获取 dubbo 封装的对象流
        ObjectInput in = CodecSupport.getSerialization(channel.getUrl(), serializationType)
                .deserialize(channel.getUrl(), input);

        //从对象流中获取 版本
        String dubboVersion = in.readUTF();
        request.setVersion(dubboVersion);
        //设置 attachement属性
        setAttachment(Constants.DUBBO_VERSION_KEY, dubboVersion);

        setAttachment(Constants.PATH_KEY, in.readUTF());
        setAttachment(Constants.VERSION_KEY, in.readUTF());

        //设置 方法
        setMethodName(in.readUTF());
        try {
            Object[] args;
            Class<?>[] pts;
            //如果获取不到方法描述就是空方法
            String desc = in.readUTF();
            if (desc.length() == 0) {
                pts = DubboCodec.EMPTY_CLASS_ARRAY;
                args = DubboCodec.EMPTY_OBJECT_ARRAY;
            } else {
                //将描述 转换成 方法参数类型
                pts = ReflectUtils.desc2classArray(desc);
                //方法参数列表
                args = new Object[pts.length];
                for (int i = 0; i < args.length; i++) {
                    try {
                        //该对象流 可以通过传入参数类型获取对应的数据
                        args[i] = in.readObject(pts[i]);
                    } catch (Exception e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Decode argument failed: " + e.getMessage(), e);
                        }
                    }
                }
            }
            //设置参数类型
            setParameterTypes(pts);

            //从对象流中获取存放属性的  map 容器
            Map<String, String> map = (Map<String, String>) in.readObject(Map.class);
            if (map != null && map.size() > 0) {
                //获取 attachment  一般就是 null
                Map<String, String> attachment = getAttachments();
                if (attachment == null) {
                    attachment = new HashMap<String, String>();
                }
                //给 附属对象 增加 参数
                attachment.putAll(map);
                //重新设置
                setAttachments(attachment);
            }
            //decode argument ,may be callback  为参数 解码
            for (int i = 0; i < args.length; i++) {
                //对参数做一些 处理 根据是否是 异步参数
                args[i] = decodeInvocationArgument(channel, this, pts, i, args[i]);
            }

            //将处理完的参数 设置
            setArguments(args);

        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read invocation data failed.", e));
        } finally {
            if (in instanceof Cleanable) {
                ((Cleanable) in).cleanup();
            }
        }
        return this;
    }

}
