package com.netflix.dyno.contrib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.discovery.DiscoveryClient;

/**
 * #ref-gov-instance-holder
 *
 * One way to simplify app governation is to store all required libs in a holder class, instead of injecting
 * them directly in the code, and then use this holder class to access them in the code as needed.
 *
 * There are two options to store these libs:
 * Option 1: if the number of libs is small, we can simply store them as member properties
 * Option 2: if the number of libs is large, we can store them in a map
 * Created by nabbas on 4/1/14.
 */
public class LibInstanceHolder {

	private static final Logger Logger = LoggerFactory.getLogger(LibInstanceHolder.class);
	
    // option 1
    private static DiscoveryClient dc;

    @Inject
    LibInstanceHolder(DiscoveryClient dcLib) 
    {
        //option 1
        this.dc = dcLib;
        Logger.info("DC CLIENT INITED: " + dc);
    }

    //option 1
    public static DiscoveryClient getDiscoveryClient() {
        return dc;
    }
}
