/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

/*
 * Copyright (c) 2023 OceanBase.
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

package com.alipay.oceanbase.rpc.containerBase;

import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;

import com.github.dockerjava.api.model.ContainerNetwork;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.Arrays;

public class ContainerTestBase {

    private static final Logger logger = LoggerFactory
            .getLogger(ContainerTestBase.class);
    protected static final int CONFIG_SERVER_PORT = 8080;
    protected static final String CONFIG_URL_PATH = "/services?Action=GetObProxyConfig";
    protected static final Network NETWORK = Network.newNetwork();
    protected static final String CLUSTER_NAME = "obkv-table-client-java";
    protected static final String SYS_PASSWORD = "OB_ROOT_PASSWORD";
    protected static final String TEST_USERNAME = "root@test#" + CLUSTER_NAME;


    @SuppressWarnings("resource")
    public static final GenericContainer<?> CONFIG_SERVER =
            new GenericContainer<>("oceanbase/ob-configserver:1.0.0-2")
                    .withNetwork(NETWORK)
                    .withExposedPorts(CONFIG_SERVER_PORT)
                    .waitingFor(
                            new HttpWaitStrategy()
                                    .forPort(CONFIG_SERVER_PORT)
                                    .forPath(CONFIG_URL_PATH));

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected static GenericContainer createOceanBaseCEContainer() {
        GenericContainer container = new GenericContainer("oceanbase/oceanbase-ce:latest")
                .withNetwork(NETWORK)
                .withExposedPorts(2881, 2882, 8080)
                .withEnv("MODE", "slim")
                .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                .withEnv("OB_ROOT_PASSWORD", SYS_PASSWORD)
                .withCopyFileToContainer(MountableFile.forClasspathResource("ci.sql"),
                        "/root/boot/init.d/init.sql")
                .waitingFor(
                        Wait.forLogMessage(".*boot success!.*", 1)
                                .withStartupTimeout(Duration.ofMinutes(5)))
                .withLogConsumer(new Slf4jLogConsumer(logger));
        container.setPortBindings(Arrays.asList("2881:2881", "2882:2882", "8080:8080"));
        return container;
    }

    public static String getConfigServerAddress() {
        CONFIG_SERVER.start();
        return getConfigServerAddress(CONFIG_SERVER);
    }

    public static String getConfigServerAddress(GenericContainer<?> container) {
        String ip = getContainerIP(container);
        return "http://" + ip + ":" + CONFIG_SERVER_PORT;
    }

    public static String getContainerIP(GenericContainer<?> container) {
        String ip =
                container.getContainerInfo().getNetworkSettings().getNetworks().values().stream()
                        .findFirst()
                        .map(ContainerNetwork::getIpAddress)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Can't get IP address of container: " + container));
        return ip;
    }

    @BeforeClass
    public static void before() {
        CONFIG_SERVER.start();
        createOceanBaseCEContainer().withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress()).start();
        if (!ObTableClientTestUtil.FULL_USER_NAME.equals("full-user-name")) {
            return;
        }

        // Set config
        ObTableClientTestUtil.PARAM_URL = "http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster="
                + CLUSTER_NAME + "&database=test";
        ObTableClientTestUtil.FULL_USER_NAME = TEST_USERNAME;
        ObTableClientTestUtil.PASSWORD = "";
        ObTableClientTestUtil.PROXY_SYS_USER_NAME = "root";
        ObTableClientTestUtil.PROXY_SYS_USER_PASSWORD = SYS_PASSWORD;
    }
}
