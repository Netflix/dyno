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
package com.netflix.dyno.contrib;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolConfigurationPublisher;

/**
 * Publishes connection pool configuration information to an elastic cluster using elastic's REST interface.
 *
 * @author jcacciatore
 */
public class ElasticConnectionPoolConfigurationPublisher implements ConnectionPoolConfigurationPublisher {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(ElasticConnectionPoolConfigurationPublisher.class);

    private final String applicationName;
    private final String clusterName;
    private final String vip;
    private final ConnectionPoolConfiguration config;

    private static final String DESTINATION = "http://%s:7104/clientinfo/dyno";

    public ElasticConnectionPoolConfigurationPublisher(String applicationName, String clusterName, String vip,
                                                       ConnectionPoolConfiguration config) {
        this.applicationName = applicationName;
        this.clusterName = clusterName;
        this.vip = vip;
        this.config = config;
    }

    /**
     * See {@link ConnectionPoolConfigurationPublisher#publish()}
     */
    @Override
    public void publish() {
        try {
            String destination = String.format(DESTINATION, vip);
            executePost(destination, createJsonFromConfig(config));
        } catch (JSONException e) {
            /* forget it */
        }
    }

    JSONObject createJsonFromConfig(ConnectionPoolConfiguration config) throws JSONException {
        if (config == null) {
            throw new IllegalArgumentException("Valid ConnectionPoolConfiguration instance is required");
        }

        JSONObject json = new JSONObject();

        json.put("ApplicationName", applicationName);
        json.put("DynomiteClusterName", clusterName);

        Method[] methods = config.getClass().getMethods();
        for (Method method: methods) {
            if (method.getName().startsWith("get")) {
                Class<?> ret = method.getReturnType();
                if (ret.isPrimitive() || ret == java.lang.String.class) {
                    try {
                        json.put(method.getName().substring(3), method.invoke(config));
                    } catch (ReflectiveOperationException ex) {
                        /* forget it */
                    }
                }
            }
        }

        // jar version
        Set<String> jars = new HashSet<String>() {{
            add("dyno-core");
            add("dyno-contrib");
            add("dyno-jedis");
            add("dyno-reddison");
        }};

        json.put("Versions", new JSONObject(getLibraryVersion(this.getClass(), jars)));

        return json;
    }

    /**
     *
     * @param destination
     * @param jsonObject
     */
    void executePost(String destination, JSONObject jsonObject) {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(destination);

        try {
            StringEntity entity = new StringEntity(jsonObject.toString());
            httpPost.setEntity(entity);
            HttpResponse response = httpclient.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            if (statusLine != null) {
                if (200 >= statusLine.getStatusCode() && statusLine.getStatusCode() < 300) {
                    Logger.info("successfully published runtime data to " + DESTINATION);
                } else {
                    Logger.info(String.format("unable to publish runtime data: %d %s ", statusLine.getStatusCode(),
                            statusLine.getReasonPhrase()));
                }
            }
        } catch (UnsupportedEncodingException ue) {
            Logger.warn("Unable to create entity to post from json " + jsonObject.toString());
        } catch (IOException e) {
            Logger.warn("Unable to post configuration data to elastic cluster: " + destination);
        } catch (Throwable th) {
            Logger.warn("Unable to post configuration data to elastic cluster:" + th.getMessage());
        } finally {
            httpPost.releaseConnection();
        }
    }

    /**
     * Get library version by iterating through the classloader jar list and obtain the name from the jar filename.
     * This will not open the library jar files.
     * <p>
     * This function assumes the conventional jar naming format and relies on the dash character to separate the
     * name of the jar from the version. For example, foo-bar-baz-1.0.12-CANDIDATE.
     *
     * @param libraryNames unique list of library names, i.e. "dyno-core"
     * @param classLoadedWithURLClassLoader For this to work, must have a URL based classloader
     *        This has been tested to be the case in Tomcat (WebAppClassLoader) and basic J2SE classloader
     * @return the version of the library (everything between library name, dash, and .jar)
     */
    Map<String, String> getLibraryVersion(Class<?> classLoadedWithURLClassLoader, Set<String> libraryNames) {
        ClassLoader cl = classLoadedWithURLClassLoader.getClassLoader();
        Map<String, String> libraryVersionMapping = new HashMap<String, String>();

        if (cl instanceof URLClassLoader) {
            @SuppressWarnings("resource")
            URLClassLoader uCl = (URLClassLoader) cl;
            URL urls[] = uCl.getURLs();

            for (URL url : urls) {
                String fullNameWithVersion = url.toString().substring(url.toString().lastIndexOf('/'));
                if (fullNameWithVersion.length() > 4) { // all entries we attempt to parse must end in ".jar"
                    String nameWithVersion = fullNameWithVersion.substring(1, fullNameWithVersion.length() - 4);
                    int idx = findVersionStartIndex(nameWithVersion);
                    if (idx > 0) {
                        String name = nameWithVersion.substring(0, idx - 1);
                        if (libraryNames.contains(name)) {
                            libraryVersionMapping.put(name, nameWithVersion.substring(idx));
                        }
                    }
                }
            }
        }

        return libraryVersionMapping;
    }

    Map<String, String> getLibraryVersion(Class<?> classLoadedWithURLClassLoader, String... libraryNames) {
        return getLibraryVersion(classLoadedWithURLClassLoader, new HashSet<String>(Arrays.asList(libraryNames)));
    }

    int findVersionStartIndex(String nameWithVersion) {
        if (nameWithVersion != null) {
            char[] chars = nameWithVersion.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == '-') {
                    if (i < chars.length && Character.isDigit(chars[i + 1])) {
                        return i + 1;
                    }
                }
            }
        }

        return -1;
    }

}
