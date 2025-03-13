/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.utils.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Endpoint is what fluss server is listened for. It includes host, port and listener name. Listener
 * name is used for routing, all the fluss servers can have the same listener names to listen. For
 * example, coordinator server and tablet sever can use internal listener to communicate with each
 * other. And If a client connect to a server with a host and port, it can only see the other
 * server's same listener.
 */
@Internal
public class Endpoint {
    private static final Pattern ENDPOINT_PARSE_EXP =
            Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

    private final String host;
    private final int port;
    private final String listenerName;

    public Endpoint(String host, int port, String listenerName) {
        this.host = host;
        this.port = port;
        this.listenerName = listenerName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getListenerName() {
        return listenerName;
    }

    /**
     * Loads the tablet server endpoints based on the provided configuration.
     *
     * <p>This method checks if the {@code bind.listeners} configuration is present. If so, it
     * parses the listeners string to generate the endpoint list. Otherwise, it falls back to
     * deprecated configurations like {@code tablet-server.host} and {@code tablet-server.port},
     * while ensuring that {@code internal.listener.name} is not set without {@code bind.listeners}.
     *
     * @param conf the configuration object containing the server settings
     * @return a list of {@link Endpoint} representing the tablet server's listening addresses
     * @throws IllegalArgumentException if no valid listener or endpoint configuration is found
     */
    public static List<Endpoint> loadTabletServerEndpoints(Configuration conf) {
        List<Endpoint> endpoints;
        if (conf.getOptional(ConfigOptions.BIND_LISTENERS).isPresent()
                || !conf.getOptional(ConfigOptions.TABLET_SERVER_HOST).isPresent()) {
            return Endpoint.fromListenersString(conf.getString(ConfigOptions.BIND_LISTENERS));
        } else {
            if (conf.getOptional(ConfigOptions.INTERNAL_LISTENER_NAME).isPresent()) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s cannot set without %s",
                                ConfigOptions.INTERNAL_LISTENER_NAME.key(),
                                ConfigOptions.BIND_LISTENERS.key()));
            }
            endpoints =
                    Collections.singletonList(
                            new Endpoint(
                                    conf.get(ConfigOptions.TABLET_SERVER_HOST),
                                    Integer.parseInt(
                                            conf.getString(ConfigOptions.TABLET_SERVER_PORT)),
                                    ConfigOptions.INTERNAL_LISTENER_NAME.defaultValue()));
        }

        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("No coordinator server listeners are configured");
        }

        return endpoints;
    }

    /**
     * Loads the tablet server endpoints based on the provided configuration.
     *
     * <p>This method checks if the {@code bind.listeners} configuration is present. If so, it
     * parses the listeners string to generate the endpoint list. Otherwise, it falls back to
     * deprecated configurations like {@code tablet-server.host} and {@code tablet-server.port},
     * while ensuring that {@code internal.listener.name} is not set without {@code bind.listeners}.
     *
     * @param conf the configuration object containing the server settings
     * @return a list of {@link Endpoint} representing the tablet server's listening addresses
     * @throws IllegalArgumentException if no valid listener or endpoint configuration is found
     */
    public static List<Endpoint> loadCoordinatorServerEndpoints(Configuration conf) {
        if (conf.getOptional(ConfigOptions.BIND_LISTENERS).isPresent()
                || !conf.getOptional(ConfigOptions.COORDINATOR_HOST).isPresent()) {
            return Endpoint.fromListenersString(conf.getString(ConfigOptions.BIND_LISTENERS));
        } else {
            if (conf.getOptional(ConfigOptions.INTERNAL_LISTENER_NAME).isPresent()) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s cannot set without %s",
                                ConfigOptions.INTERNAL_LISTENER_NAME.key(),
                                ConfigOptions.BIND_LISTENERS.key()));
            }
            return Collections.singletonList(
                    new Endpoint(
                            conf.get(ConfigOptions.COORDINATOR_HOST),
                            Integer.parseInt(conf.getString(ConfigOptions.COORDINATOR_PORT)),
                            ConfigOptions.INTERNAL_LISTENER_NAME.defaultValue()));
        }
    }

    public static List<Endpoint> fromListenersString(String listeners) {
        if (StringUtils.isNullOrWhitespaceOnly(listeners)) {
            return Collections.emptyList();
        }
        return Arrays.stream(listeners.split(","))
                .map(Endpoint::fromConnectionString)
                .collect(Collectors.toList());
    }

    /**
     * Create Endpoint object from {@code connectionString}.
     *
     * @param connectionString the format is listener_name://host:port or listener_name://[ipv6
     *     host]:port for example: INTERNAL://my_host:9092, CLIENT://my_host:9093 or
     *     REPLICATION://[::1]:9094
     */
    private static Endpoint fromConnectionString(String connectionString) {
        Matcher matcher = ENDPOINT_PARSE_EXP.matcher(connectionString.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid endpoint format: " + connectionString);
        }

        return new Endpoint(matcher.group(2), Integer.parseInt(matcher.group(3)), matcher.group(1));
    }

    public static String toListenersString(List<Endpoint> endpoints) {
        return endpoints.stream().map(Endpoint::connectionString).collect(Collectors.joining(","));
    }

    public static List<Endpoint> getRegisteredEndpoint(
            List<Endpoint> bindEndpoints, List<Endpoint> advisedEndpoints) {
        Map<String, Endpoint> advisedEndpointMap =
                advisedEndpoints.stream()
                        .collect(Collectors.toMap(Endpoint::getListenerName, endpoint -> endpoint));
        return bindEndpoints.stream()
                .map(
                        endpoint ->
                                advisedEndpointMap.getOrDefault(
                                        endpoint.getListenerName(), endpoint))
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Endpoint endpoint = (Endpoint) o;
        return port == endpoint.port
                && Objects.equals(host, endpoint.host)
                && Objects.equals(listenerName, endpoint.listenerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, listenerName);
    }

    public String connectionString() {
        return listenerName + "://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return connectionString();
    }
}
