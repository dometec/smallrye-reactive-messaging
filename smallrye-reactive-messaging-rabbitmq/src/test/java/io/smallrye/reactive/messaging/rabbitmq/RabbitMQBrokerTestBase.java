package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.se.SeContainer;

import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

/**
 * Provides a basis for test classes, by managing the RabbitMQ broker test container.
 */
@ExtendWith(RabbitMQBrokerExtension.class)
public class RabbitMQBrokerTestBase {

    protected static String host;
    protected static int port;
    protected static int managementPort;
    final static String username = "guest";
    final static String password = "guest";
    protected RabbitMQUsage usage;
    ExecutionHolder executionHolder;

    protected String exchangeName;
    protected String queueName;

    @BeforeAll
    public static void initBroker(
            @RabbitMQBrokerExtension.RabbitMQHost String h,
            @RabbitMQBrokerExtension.RabbitMQPort int p,
            @RabbitMQBrokerExtension.RabbitMQManagementPort int mp) {
        host = h;
        port = p;
        managementPort = mp;

        System.setProperty("rabbitmq-host", host);
        System.setProperty("rabbitmq-port", Integer.toString(port));
        System.setProperty("rabbitmq-username", username);
        System.setProperty("rabbitmq-password", password);
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new RabbitMQUsage(executionHolder.vertx(), host, port, managementPort, username, password);
        SmallRyeConfigTestUtil.releaseConfig();
        MapBasedConfig.cleanup();
    }

    @BeforeEach
    public void initQueueExchange(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queueName = "queue" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
        exchangeName = "exchange" + cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigTestUtil.releaseConfig();
        MapBasedConfig.cleanup();
    }

    /**
     * Indicates whether the connector has completed startup (including establishment of exchanges, queues
     * and so forth)
     *
     * @param container the {@link WeldContainer}
     * @return true if the connector is ready, false otherwise
     */
    public boolean isRabbitMQConnectorAvailable(WeldContainer container) {
        final RabbitMQConnector connector = get(container, RabbitMQConnector.class, Any.Literal.INSTANCE);
        return connector.getLiveness().isOk();
    }

    /**
     * The connector declares queues and their bindings asynchronously, and reports itself available and ready
     * before they exist, while an exchange drops the messages it cannot route. Wait for the exchange to have a
     * binding for each of the given routing keys, repeated as many times as it is given, before producing.
     *
     * @param exchange the name of the exchange
     * @param routingKeys the routing keys of the expected bindings
     */
    public void awaitExchangeBindings(String exchange, String... routingKeys) {
        await().untilAsserted(() -> {
            JsonArray bindings = usage.getExchangeBindings(exchange);
            assertThat(bindings).isNotNull();
            List<String> bound = new ArrayList<>();
            bindings.forEach(binding -> bound.add(((JsonObject) binding).getString("routing_key")));
            for (String routingKey : routingKeys) {
                assertThat(bound.remove(routingKey))
                        .as("binding with routing key '%s' of exchange %s in %s", routingKey, exchange, bindings)
                        .isTrue();
            }
        });
    }

    public boolean isRabbitMQConnectorReady(SeContainer container) {
        HealthCenter health = get(container, HealthCenter.class);
        return health.getReadiness().isOk();
    }

    public boolean isRabbitMQConnectorAlive(SeContainer container) {
        HealthCenter health = get(container, HealthCenter.class);
        return health.getLiveness().isOk();
    }

    public <T> T get(SeContainer container, Class<T> beanType, Annotation... annotations) {
        return container.getBeanManager().createInstance().select(beanType, annotations).get();
    }

    public MapBasedConfig commonConfig() {
        return new MapBasedConfig()
                .with("rabbitmq-host", host)
                .with("rabbitmq-port", port)
                .with("rabbitmq-username", username)
                .with("rabbitmq-password", password)
                .with("rabbitmq-reconnect-attempts", 0);
    }
}
