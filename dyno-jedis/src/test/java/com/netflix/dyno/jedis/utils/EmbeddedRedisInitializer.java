package com.netflix.dyno.jedis.utils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;


import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public class EmbeddedRedisInitializer extends ChannelInitializer<SocketChannel> {
    private final SSLContext sslContext;
    private final boolean useSsl;
    private final String response;

    public EmbeddedRedisInitializer(final SSLContext sslContext, final boolean useSsl, final String response) {
        this.sslContext = sslContext;
        this.useSsl = useSsl;
        this.response = response;
    }

    @Override
    protected void initChannel(final SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        if (useSsl) {
            final SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
            sslEngine.getNeedClientAuth();

            pipeline.addLast("sslHandler", new SslHandler(sslEngine));
        }


        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());

        pipeline.addLast(new MockedResponseHandler(response));
    }
}
