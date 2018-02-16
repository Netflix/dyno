/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.contrib.consul;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.ecwid.consul.v1.health.model.HealthService;

/**
 * 
 * Class with support for consul simple/commons operations
 */
public class ConsulHelper {

    public static String findHost(HealthService healthService) {
        HealthService.Service service = healthService.getService();
        HealthService.Node node = healthService.getNode();

        if (StringUtils.isNotBlank(service.getAddress())) {
            return service.getAddress();
        } else if (StringUtils.isNotBlank(node.getAddress())) {
            return node.getAddress();
        }
        return node.getNode();
    }

    public static Map<String, String> getMetadata(HealthService healthService) {
        return getMetadata(healthService.getService().getTags());
    }

    public static Map<String, String> getMetadata(List<String> tags) {
        LinkedHashMap<String, String> metadata = new LinkedHashMap<>();
        if (tags != null) {
            for (String tag : tags) {
                String[] parts = StringUtils.split(tag, "=");
                switch (parts.length) {
                case 0:
                    break;
                case 1:
                    metadata.put(parts[0], parts[0]);
                    break;
                case 2:
                    metadata.put(parts[0], parts[1]);
                    break;
                default:
                    String[] end = Arrays.copyOfRange(parts, 1, parts.length);
                    metadata.put(parts[0], StringUtils.join(end, "="));
                    break;
                }

            }
        }

        return metadata;
    }
}