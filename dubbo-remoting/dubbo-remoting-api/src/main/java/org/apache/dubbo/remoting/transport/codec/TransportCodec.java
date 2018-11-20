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
package org.apache.dubbo.remoting.transport.codec;

import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.transport.AbstractCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * TransportCodec
 * 协议层的 编解码器
 */
public class TransportCodec extends AbstractCodec {

    /**
     * 编码
     * @param channel
     * @param buffer
     * @param message
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException {
        //从 channelBuffer 中获取输出流
        OutputStream output = new ChannelBufferOutputStream(buffer);
        //通过 channel 的 url 获取 序列化实现类 并 调用序列化方法 获取对象流
        ObjectOutput objectOutput = getSerialization(channel).serialize(channel.getUrl(), output);
        //对获得 的 对象进行编码
        encodeData(channel, objectOutput, message);
        objectOutput.flushBuffer();
        //释放资源
        if (objectOutput instanceof Cleanable) {
            ((Cleanable) objectOutput).cleanup();
        }
    }

    /**
     * 解码
     * @param channel
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        //创建 输入流对象
        InputStream input = new ChannelBufferInputStream(buffer);
        //获取 对象流
        ObjectInput objectInput = getSerialization(channel).deserialize(channel.getUrl(), input);
        //解码
        Object object = decodeData(channel, objectInput);
        if (objectInput instanceof Cleanable) {
            ((Cleanable) objectInput).cleanup();
        }
        return object;
    }

    /**
     * 将数据 写入到 ObjectOutput
     * @param channel
     * @param output
     * @param message
     * @throws IOException
     */
    protected void encodeData(Channel channel, ObjectOutput output, Object message) throws IOException {
        encodeData(output, message);
    }

    /**
     * 解码
     * @param channel
     * @param input
     * @return
     * @throws IOException
     */
    protected Object decodeData(Channel channel, ObjectInput input) throws IOException {
        return decodeData(input);
    }

    /**
     * 将 message 写入到 ObjectOutput
     * @param output
     * @param message
     * @throws IOException
     */
    protected void encodeData(ObjectOutput output, Object message) throws IOException {
        output.writeObject(message);
    }

    protected Object decodeData(ObjectInput input) throws IOException {
        try {
            return input.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("ClassNotFoundException: " + StringUtils.toString(e));
        }
    }
}
