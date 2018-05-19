package com.netflix.dyno.connectionpool;

import java.util.Map;

public interface TokenRackMapper {
    Map<Long, String> getTokenRackMap();

    String getRackForToken(Long Token);

    void setRackForToken(Long token, String rack);

}
