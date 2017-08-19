package com.netflix.dyno.jedis.utils;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MockedResponseHandler extends SimpleChannelInboundHandler<String> {

    private final String response;

    public MockedResponseHandler(final String response) {
        this.response = response;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final String msg) throws Exception {
        
        ctx.writeAndFlush("$" + response.length() + "\r\n");
        ctx.writeAndFlush(response + "\r\n");
    }
}
