/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.redisson;

import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dyno/demo/redisson")
public class DynoRedissonDemoResource {

    private static final Logger Logger = LoggerFactory.getLogger(DynoRedissonDemoResource.class);

    private static final AtomicReference<RedissonDemo> demo = new AtomicReference<RedissonDemo>(null);

    public DynoRedissonDemoResource() {
    }

    @Path("/start/{threads}/{loop}")
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String startRedisson(@PathParam("threads") int nThreads, @PathParam("loop") int loop) throws Exception {

        Logger.info("Starting redisson demo");
        try {
            demo.set(new RedissonDemo(nThreads, loop));
            demo.get().run();
            return "redisson demo!" + "\n";
        } catch (Exception e) {
            Logger.error("Error starting datafill", e);
            return "redisson demo failed";
        }
    }

    @Path("/stop")
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String stopRedisson() throws Exception {

        Logger.info("stopping redisson demo");
        try {
            demo.get().stop();
            return "stop!" + "\n";
        } catch (Exception e) {
            Logger.error("Error starting datafill", e);
            return "redisson demo failed";
        }
    }
}
