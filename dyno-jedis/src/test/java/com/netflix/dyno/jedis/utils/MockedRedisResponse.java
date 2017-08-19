package com.netflix.dyno.jedis.utils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import javax.net.ssl.SSLContext;

import static com.netflix.dyno.jedis.utils.SSLContextUtil.createAndInitSSLContext;


/**
 * Simple server that will response with pre-configured data.
 */
public class MockedRedisResponse {

    private final String response;
    private final boolean useSsl;

    private ServerBootstrap serverBootstrap;

    private Channel serverChannel;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    public MockedRedisResponse(final String response, final boolean useSsl)
    {
        this.response = response;
        this.useSsl = useSsl;
    }

    public void start() throws Exception
    {
        final SSLContext sslContext = createAndInitSSLContext("server.jks");

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)//
                .channel(NioServerSocketChannel.class) //
                .handler(new LoggingHandler(LogLevel.INFO)) //
                .childHandler(new EmbeddedRedisInitializer(sslContext,useSsl,response));

        serverChannel = serverBootstrap.bind(8998).sync().channel();
    }

    public void stop() throws InterruptedException {
        serverChannel.close().sync();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
