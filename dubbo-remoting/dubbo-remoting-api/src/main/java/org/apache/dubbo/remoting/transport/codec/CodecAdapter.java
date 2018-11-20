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

import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.io.UnsafeByteArrayOutputStream;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import java.io.IOException;

/**
 * 适配器 将 code 转换成 code2
 */
public class CodecAdapter implements Codec2 {

    private Codec codec;

    public CodecAdapter(Codec codec) {
        Assert.notNull(codec, "codec == null");
        this.codec = codec;
    }

    /**
     * 编码
     * @param channel
     * @param buffer
     * @param message
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object message)
            throws IOException {
        //创建 特殊输出流
        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream(1024);
        //编码后 将数据写入到里面
        codec.encode(channel, os, message);
        buffer.writeBytes(os.toByteArray());
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
        //创建 等大 的数组对象
        byte[] bytes = new byte[buffer.readableBytes()];
        //获取 buffer当前指针
        int savedReaderIndex = buffer.readerIndex();
        buffer.readBytes(bytes);
        //用读取的数据 创建一个输入流对象
        UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(bytes);
        //解析出结果
        Object result = codec.decode(channel, is);
        //移动指针
        buffer.readerIndex(savedReaderIndex + is.position());
        return result == Codec.NEED_MORE_INPUT ? DecodeResult.NEED_MORE_INPUT : result;
    }

    public Codec getCodec() {
        return codec;
    }
}
