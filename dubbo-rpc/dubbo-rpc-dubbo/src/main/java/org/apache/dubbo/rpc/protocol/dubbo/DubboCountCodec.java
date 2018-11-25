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
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;

import java.io.IOException;

/**
 * dubbo 的 计数器 编解码对象
 */
public final class DubboCountCodec implements Codec2 {

    /**
     * 编解码对象
     */
    private DubboCodec codec = new DubboCodec();

    /**
     * 编码方法
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        //委托 codec 实现编码
        codec.encode(channel, buffer, msg);
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
        //获取 buffer 读指针
        int save = buffer.readerIndex();
        //创建 复合消息  就是一个 List<Message>
        MultiMessage result = MultiMessage.create();
        do {
            //委托 解码
            Object obj = codec.decode(channel, buffer);
            //无法解码时
            if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {
                //还原指针
                buffer.readerIndex(save);
                break;
            } else {
                //将结果保存到 result中 同时 更新 读指针
                result.addMessage(obj);
                //为每个 解码后产生的对象设置长度
                logMessageLength(obj, buffer.readerIndex() - save);
                save = buffer.readerIndex();
            }
        } while (true);
        if (result.isEmpty()) {
            //没有解析到数据 需要更多输入
            return Codec2.DecodeResult.NEED_MORE_INPUT;
        }
        if (result.size() == 1) {
            return result.get(0);
        }
        return result;
    }

    /**
     * 记录消息长度
     * @param result
     * @param bytes
     */
    private void logMessageLength(Object result, int bytes) {
        if (bytes <= 0) {
            return;
        }
        //是请求类型
        if (result instanceof Request) {
            try {
                // 将长度 保存到 input 中
                ((RpcInvocation) ((Request) result).getData()).setAttachment(
                        Constants.INPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
            //响应类型  保存 output 的长度
        } else if (result instanceof Response) {
            try {
                ((RpcResult) ((Response) result).getResult()).setAttachment(
                        Constants.OUTPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        }
    }

}
