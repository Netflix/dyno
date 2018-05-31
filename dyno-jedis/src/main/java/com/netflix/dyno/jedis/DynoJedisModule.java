package com.netflix.dyno.jedis;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

@Singleton
public class DynoJedisModule extends AbstractModule {

    public DynoJedisModule() {
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

    }

}
