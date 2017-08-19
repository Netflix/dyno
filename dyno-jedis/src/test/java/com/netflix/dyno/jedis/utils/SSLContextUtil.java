package com.netflix.dyno.jedis.utils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

public class SSLContextUtil {
    //FIXME - generate self-signed certs with appropriate password :D
    private static final String STOREPASS = "tutorial123";

    public static final SSLContext createAndInitSSLContext(final String jksFileName) throws Exception {
        final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        final InputStream inputStream = classloader.getResourceAsStream(jksFileName);

        final KeyStore trustStore = KeyStore.getInstance("jks");
        trustStore.load(inputStream, STOREPASS.toCharArray());

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(trustStore, STOREPASS.toCharArray());

        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

        return sslContext;
    }
}
