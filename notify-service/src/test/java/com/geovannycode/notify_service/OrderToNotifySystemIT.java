package com.geovannycode.notify_service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import com.geovannycode.notify_service.generated.dto.NotifyResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The three services for real: Inventory and Order from their images (with MySQL and PostgreSQL), Notification in
 * this JVM, Kafka and MongoDB in Testcontainers. Runs only with -Psystem-tests. Same approach as Order's
 * OrderInventorySystemIT: images built beforehand, one Docker network, the Compose digests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers
final class OrderToNotifySystemIT {

    private static final String INVENTORY_IMAGE =
            System.getProperty("inventory.image", "geovannycode/service-inventory:0.0.1-SNAPSHOT");
    private static final String ORDER_IMAGE =
            System.getProperty("order.image", "geovannycode/service-order:0.0.1-SNAPSHOT");
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final Duration STARTUP = Duration.ofMinutes(3);

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final GenericContainer<?> MYSQL = new GenericContainer<>(
            // mysql:8.4.11, same digest as docker-compose.yaml.
            "mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242")
            .withNetwork(NETWORK).withNetworkAliases("mysql")
            .withEnv(Map.of("MYSQL_ROOT_PASSWORD", "root", "MYSQL_DATABASE", "inventory",
                    "MYSQL_USER", "inventory", "MYSQL_PASSWORD", "inventory"))
            // The entrypoint first starts a temporary server without networking (port: 0); wait for the real one.
            .waitingFor(Wait.forLogMessage(".*ready for connections.*port: 3306.*", 1).withStartupTimeout(STARTUP));

    @Container
    static final GenericContainer<?> POSTGRES = new GenericContainer<>(
            // postgres:17.11, same digest as docker-compose.yaml.
            "postgres@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f")
            .withNetwork(NETWORK).withNetworkAliases("postgresql")
            .withEnv(Map.of("POSTGRES_DB", "orderdb", "POSTGRES_USER", "order", "POSTGRES_PASSWORD", "order"))
            // Logged twice: once by the init server, once by the real one.
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
                    .withStartupTimeout(STARTUP));

    // Two listeners: the host one for Notification (this JVM, via @ServiceConnection) and kafka:19092 for Order.
    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(
            "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837")
            .withNetwork(NETWORK).withNetworkAliases("kafka").withListener("kafka:19092");

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer(
            "mongo@sha256:d0d926f94df099bff534b7ee5b5986458131a22489dfff8664509af0c1e2ca9c");

    @Container
    static final GenericContainer<?> INVENTORY = new GenericContainer<>(requireImage(INVENTORY_IMAGE, "inventory-service"))
            .withNetwork(NETWORK).withNetworkAliases("inventory")
            .dependsOn(MYSQL)
            .withEnv(Map.of("SPRING_PROFILES_ACTIVE", "docker", "DB_HOST", "mysql", "DB_NAME", "inventory",
                    "DB_USER", "inventory", "DB_PASSWORD", "inventory", "MANAGEMENT_TRACING_EXPORT_ENABLED", "false"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/services-inventory/actuator/health/readiness").forStatusCode(200)
                    .withStartupTimeout(STARTUP));

    // Order's docker profile creates orders.events.v1 (3 partitions) on startup, before Notification subscribes.
    @Container
    static final GenericContainer<?> ORDER = new GenericContainer<>(requireImage(ORDER_IMAGE, "order-service"))
            .withNetwork(NETWORK)
            .dependsOn(POSTGRES, KAFKA, INVENTORY)
            .withEnv(Map.of("SPRING_PROFILES_ACTIVE", "docker", "DB_HOST", "postgresql", "DB_NAME", "orderdb",
                    "DB_USER", "order", "DB_PASSWORD", "order", "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092",
                    "INVENTORY_BASE_URL", "http://inventory:8080/services-inventory",
                    "MANAGEMENT_TRACING_EXPORT_ENABLED", "false"))
            .withExposedPorts(8081)
            .waitingFor(Wait.forHttp("/services-order/actuator/health/readiness").forStatusCode(200)
                    .withStartupTimeout(STARTUP));

    /**
     * The Dockerfiles need BuildKit (named build context for the contracts and cache mounts), which Testcontainers'
     * ImageFromDockerfile does not support, so the images must be built beforehand with the Docker CLI.
     */
    private static String requireImage(String image, String module) {
        try {
            DockerClientFactory.instance().client().inspectImageCmd(image).exec();
            return image;
        } catch (NotFoundException missing) {
            throw new IllegalStateException("No existe la imagen " + image + ". Constrúyela desde la raíz del repo: "
                    + "docker build --build-context contracts=contracts -t " + image + " " + module, missing);
        }
    }

    private static String url(GenericContainer<?> container, int port, String basePath) {
        return "http://" + container.getHost() + ":" + container.getMappedPort(port) + basePath;
    }

    private static final String PRODUCT = "SYS-" + ThreadLocalRandom.current().nextInt(10_000, 99_999);

    private final WebClient order = WebClient.create(url(ORDER, 8081, "/services-order"));
    private final WebClient inventory = WebClient.create(url(INVENTORY, 8080, "/services-inventory"));
    private final WebClient notify;

    OrderToNotifySystemIT(@Value("${local.server.port}") int port) {
        this.notify = WebClient.create("http://127.0.0.1:" + port + "/services-notify");
    }

    @BeforeAll
    static void productWithStock() {
        var created = WebClient.create(url(INVENTORY, 8080, "/services-inventory")).post().uri("/inventories")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("idProduct", PRODUCT, "nameProduct", "Producto de sistema", "price", 10.00, "stock", 50))
                .retrieve().toBodilessEntity().block(TIMEOUT);
        assertThat(Objects.requireNonNull(created).getStatusCode().value()).isEqualTo(201);
    }

    @Test
    void confirmedOrderWithStockEndsInASentOrderCompletedNotification() {
        long orderId = createOrder(PRODUCT);
        assertThat(confirm(orderId)).isEqualTo(200);

        var notification = awaitSentNotification(orderId);
        assertThat(notification.getEventType().getValue()).isEqualTo("OrderCompleted");
        assertThat(notification.getCodeProduct()).isEqualTo(PRODUCT);
        assertThat(notification.getMessage()).isEqualTo("La orden " + orderId + " (" + PRODUCT + " x1) fue completada.");
        assertThat(stockOf(PRODUCT)).isLessThan(50);
    }

    @Test
    void orderForAProductThatDoesNotExistEndsInAnOrderCanceledNotificationWithTheReason() {
        long orderId = createOrder("NO-EXISTE-" + ThreadLocalRandom.current().nextInt(1_000, 9_999));
        assertThat(confirm(orderId)).isEqualTo(409);

        var notification = awaitSentNotification(orderId);
        assertThat(notification.getEventType().getValue()).isEqualTo("OrderCanceled");
        assertThat(notification.getCancelReason()).isEqualTo("PRODUCT_NOT_FOUND");
        assertThat(notification.getMessage()).endsWith("fue cancelada: el producto no existe.");
    }

    @Test
    void confirmingTheSameOrderTwiceStillGivesOneNotification() {
        long orderId = createOrder(PRODUCT);
        assertThat(confirm(orderId)).isEqualTo(200);
        assertThat(confirm(orderId)).isEqualTo(200);

        awaitSentNotification(orderId);
        // Order's relay polls its outbox every second: a second event would show up well within this window.
        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15))
                .until(() -> notificationsOf(orderId).size() == 1);
    }

    private long createOrder(String codeProduct) {
        Map<String, Object> created = order.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("codeProduct", codeProduct, "quantity", 1))
                .retrieve().bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() { }).block(TIMEOUT);
        return ((Number) Objects.requireNonNull(created).get("id")).longValue();
    }

    private int confirm(long orderId) {
        return Objects.requireNonNull(order.put().uri("/orders/{id}", orderId)
                .exchangeToMono(response -> response.releaseBody().thenReturn(response.statusCode().value()))
                .block(TIMEOUT));
    }

    private int stockOf(String code) {
        var product = inventory.get().uri("/inventories/{code}", code).retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() { }).block(TIMEOUT);
        return ((Number) Objects.requireNonNull(product).get("stock")).intValue();
    }

    private List<NotifyResponse> notificationsOf(long orderId) {
        return Objects.requireNonNull(notify.get().uri("/notify?orderId={id}", orderId).retrieve()
                .bodyToFlux(NotifyResponse.class).collectList().block(TIMEOUT));
    }

    private NotifyResponse awaitSentNotification(long orderId) {
        var notifications = await().atMost(TIMEOUT).until(() -> notificationsOf(orderId),
                found -> found.size() == 1 && "sent".equals(found.getFirst().getStatus().getValue()));
        return notifications.getFirst();
    }
}
