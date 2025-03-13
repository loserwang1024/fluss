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

package cluster;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Endpoint}. */
public class EndPointTest {

    @Test
    void testParseEndpoints() {
        List<Endpoint> parsedEndpoints =
                Endpoint.fromListenersString(
                        "INTERNAL://my_host:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9092");
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("my_host", 9092, "INTERNAL"),
                        new Endpoint("127.0.0.1", 9093, "CLIENT"),
                        new Endpoint("::1", 9092, "REPLICATION"));

        assertThat(parsedEndpoints).hasSameElementsAs(expectedEndpoints);
    }

    @Test
    void testAdvisedEndpoints() {
        List<Endpoint> registeredEndpoint =
                Endpoint.getRegisteredEndpoint(
                        Endpoint.fromListenersString(
                                "INTERNAL://127.0.0.1:9092, CLIENT://127.0.0.1:9093, REPLICATION://[::1]:9094"),
                        Endpoint.fromListenersString(
                                "CLIENT://my_host:9092,CLIENT2://my_host:9093,REPLICATION://[::1]:9094"));
        List<Endpoint> expectedEndpoints =
                Arrays.asList(
                        new Endpoint("127.0.0.1", 9092, "INTERNAL"),
                        new Endpoint("my_host", 9092, "CLIENT"),
                        new Endpoint("::1", 9094, "REPLICATION"));
        assertThat(registeredEndpoint).hasSameElementsAs(expectedEndpoints);
    }

    @Test
    void testCoordinatorEndpointsCompatibility() {
        Configuration configuration = new Configuration();
        // if no internal.listeners nor host is set, use default value of internal.listeners.
        assertThat(Endpoint.loadCoordinatorServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("localhost", 9123, "FLUSS")));
        configuration.setString(ConfigOptions.INTERNAL_LISTENER_NAME, "INTERNAL");
        configuration.setString(ConfigOptions.COORDINATOR_HOST, "my_host");
        configuration.setString(ConfigOptions.COORDINATOR_PORT, "9122");
        assertThatThrownBy(() -> Endpoint.loadCoordinatorServerEndpoints(configuration))
                .hasMessageContaining("internal.listener.name cannot set without bind.listeners");
        configuration.removeConfig(ConfigOptions.INTERNAL_LISTENER_NAME);
        assertThat(Endpoint.loadCoordinatorServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("my_host", 9122, "FLUSS")));
        // if internal.listeners is set, use it at first.
        configuration.setString(ConfigOptions.BIND_LISTENERS, "INTERNAL://127.0.0.1:9124");
        assertThat(Endpoint.loadCoordinatorServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("127.0.0.1", 9124, "INTERNAL")));
    }

    @Test
    void testTabletEndpointsCompatibility() {
        Configuration configuration = new Configuration();
        // if no internal.listeners nor host is set, use default value of internal.listeners.
        assertThat(Endpoint.loadTabletServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("localhost", 9123, "FLUSS")));
        configuration.setString(ConfigOptions.INTERNAL_LISTENER_NAME, "INTERNAL");
        configuration.setString(ConfigOptions.TABLET_SERVER_HOST, "my_host");
        configuration.setString(ConfigOptions.TABLET_SERVER_PORT, "9122");
        assertThatThrownBy(() -> Endpoint.loadTabletServerEndpoints(configuration))
                .hasMessageContaining("internal.listener.name cannot set without bind.listeners");
        configuration.removeConfig(ConfigOptions.INTERNAL_LISTENER_NAME);
        assertThat(Endpoint.loadTabletServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("my_host", 9122, "FLUSS")));
        // if internal.listeners is set, use it at first.
        configuration.setString(ConfigOptions.BIND_LISTENERS, "INTERNAL://127.0.0.1:9124");
        assertThat(Endpoint.loadTabletServerEndpoints(configuration))
                .containsExactlyElementsOf(
                        Collections.singletonList(new Endpoint("127.0.0.1", 9124, "INTERNAL")));
    }
}
