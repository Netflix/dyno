package com.netflix.dyno;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.dyno.jedis.DynoJedisModule;

@Singleton
public class DynoClientModule extends AbstractModule {
    public DynoClientModule() {
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj != null) && (obj.getClass() == getClass());
    }

    @Override
    protected void configure() {
        install(new DynoJedisModule());
    }
}
