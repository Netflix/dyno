package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostToken;

public class TokenMapSupplier {

	public TokenMapSupplier() {
		
	}
	
	public List<HostToken> getTokens() {
		
		List<HostToken> list = new ArrayList<HostToken>();
		
		String asg = System.getenv("NETFLIX_AUTO_SCALE_GROUP");

		if (asg.contains("pappy-v018")) {
			list.add(new HostToken(611697721L, new Host("ec2-107-22-154-125.compute-1.amazonaws.com", 8102, Status.Up)));
			list.add(new HostToken(2043353485L, new Host("ec2-54-205-194-198.compute-1.amazonaws.com", 8102, Status.Up)));
			list.add(new HostToken(3475009249L, new Host("ec2-54-234-199-127.compute-1.amazonaws.com", 8102, Status.Up)));
		} else {
            
			list.add(new HostToken(611697721L, new Host("ec2-54-237-33-198.compute-1.amazonaws.com", 8102, Status.Up)));
			list.add(new HostToken(2043353485L, new Host("ec2-23-23-28-219.compute-1.amazonaws.com", 8102, Status.Up)));
			list.add(new HostToken(3475009249L, new Host("ec2-54-237-223-228.compute-1.amazonaws.com", 8102, Status.Up)));
		}
		return list;
	}
	
}
