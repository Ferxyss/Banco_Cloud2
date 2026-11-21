package cl.duoc.kafka_service;

import cl.duoc.kafka_service.admin.KafkaGrupos;
import cl.duoc.kafka_service.admin.KafkaTopicos;
import cl.duoc.kafka_service.analitica.AnaliticaKafka;
import cl.duoc.kafka_service.eventos.EventosKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EmbeddedKafka(partitions = 1, topics = {
        KafkaServiceIntegrationTest.SOLICITUDES, KafkaServiceIntegrationTest.CUENTAS
}, bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {"offsets.topic.replication.factor=1", "auto.create.topics.enable=false"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.datasource.url=jdbc:h2:mem:kafka_service_test;MODE=MySQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "app.kafka.dlq.particiones=1",
        "app.kafka.dlq.replicas=1",
        "app.kafka.dlq.min-isr=1", "spring.kafka.admin.auto-create=true", "logging.level.root=WARN"
})
@Timeout(60)
class KafkaServiceIntegrationTest {

    static final String SOLICITUDES = "bancocloud.solicitudes.creadas.v1";
    static final String CUENTAS = "bancocloud.cuentas.habilitadas.v1";

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    AnaliticaConFalloTemporal analitica;

    @Autowired
    KafkaTopicos topicos;

    @Autowired
    KafkaGrupos grupos;

    @LocalServerPort
    int puerto;

    @Test
    void consumeAmbosEventosDeduplicaDerivaInvalidosYReintentaUnFalloTemporal() throws Exception {
        String solicitud = """
                {"eventId":"12a785f0-e525-43c0-9b61-3f87549edb57","solicitudId":1,
                 "tipoCuenta":"Cuenta Corriente","ocurridoEn":"2026-11-20T21:41:06.423456Z"}
                """;
        String cuenta = """
                {"eventId":"74c47ed1-3117-47ac-8d71-e18bbc0994ea","cuentaId":1,
                 "tipoCuenta":"Cuenta Corriente","ocurridoEn":"2026-11-20T23:08:03Z"}
                """;
        String invalido = "{\"solicitudId\":1}";

        kafka.send(SOLICITUDES, "1", solicitud).get(10, TimeUnit.SECONDS);
        kafka.send(SOLICITUDES, "1", solicitud).get(10, TimeUnit.SECONDS);
        kafka.send(CUENTAS, "1", cuenta).get(10, TimeUnit.SECONDS);
        kafka.send(SOLICITUDES, "1", invalido).get(10, TimeUnit.SECONDS);
        String solicitudConFalloTemporal = """
                {"eventId":"aa55d070-1aa0-4b16-9d2d-343622f67a2a","solicitudId":2,
                 "tipoCuenta":"Cuenta Prueba Reintentos","ocurridoEn":"2026-11-20T21:45:00Z"}
                """;
        kafka.send(SOLICITUDES, "2", solicitudConFalloTemporal).get(10, TimeUnit.SECONDS);

        try (var dlq = consumidor("verificacion-dlq")) {
            var destino = new TopicPartition(SOLICITUDES + ".DLT", 0);
            dlq.assign(List.of(destino));
            dlq.seekToBeginning(List.of(destino));
            await().atMost(Duration.ofSeconds(20)).until(() -> dlq.poll(Duration.ofMillis(350))
                    .records(destino).stream().anyMatch(r -> "1".equals(r.key()) && invalido.equals(r.value())));
        }

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(analitica.resumen()).containsExactlyInAnyOrder(
                        new AnaliticaKafka.Resumen("SOLICITUD_CREADA", "Cuenta Corriente", 1),
                        new AnaliticaKafka.Resumen("CUENTA_HABILITADA", "Cuenta Corriente", 1),
                        new AnaliticaKafka.Resumen("SOLICITUD_CREADA", "Cuenta Prueba Reintentos", 1)));
        assertThat(analitica.intentosDePrueba()).isEqualTo(3);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(grupos.inspeccionar("bancocloud-analitica-solicitudes-v1").progreso())
                        .anyMatch(p -> p.topico().equals(SOLICITUDES)
                                && Long.valueOf(4).equals(p.offsetConfirmado())
                                && Long.valueOf(0).equals(p.lag())));
    }

    @Test
    void administraTopicoConfiguracionParticionesYOffsets() throws Exception {
        String nombre = "ep5.admin.test";
        String grupo = "ep5-admin-test";
        var particion = new TopicPartition(nombre, 0);

        topicos.crear(new KafkaTopicos.Crear(nombre, 1, (short) 1, 60000L, "delete", 1));
        assertThat(topicos.listar()).contains(nombre);
        assertThat(topicos.inspeccionar(nombre).configuraciones().get("retention.ms").valor()).isEqualTo("60000");

        var configurado = topicos.configurar(nombre, new KafkaTopicos.Configurar(120000L, null, null, null));
        assertThat(configurado.configuraciones().get("retention.ms").valor()).isEqualTo("120000");

        var restablecido = topicos.restablecerConfiguracion(nombre, "retention.ms");
        assertThat(restablecido.configuraciones().get("retention.ms").fuente()).isNotEqualTo("DYNAMIC_TOPIC_CONFIG");
        assertThat(topicos.aumentarParticiones(nombre, 2).particiones()).hasSize(2);

        kafka.send(nombre, 0, "1", "prueba").get(10, TimeUnit.SECONDS);
        try (var consumer = consumidor(grupo)) {
            consumer.assign(List.of(particion));
            consumer.commitSync(Map.of(particion, new OffsetAndMetadata(1)));
        }

        assertThat(grupos.listar()).anyMatch(g -> g.id().equals(grupo));
        assertThat(grupos.inspeccionar(grupo).progreso())
                .anyMatch(p -> p.topico().equals(nombre) && Long.valueOf(1).equals(p.offsetConfirmado()));

        var reiniciado = grupos.restablecerOffsets(grupo, new KafkaGrupos.Restablecer(nombre, KafkaGrupos.Posicion.INICIO));
        assertThat(reiniciado.progreso()).anyMatch(p -> p.particion() == 0 && Long.valueOf(1).equals(p.lag()));

        grupos.eliminar(grupo);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(grupos.listar()).noneMatch(g -> g.id().equals(grupo)));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void eliminaTopico() {
        String nombre = "ep5.eliminacion.test";
        topicos.crear(new KafkaTopicos.Crear(nombre, 1, (short) 1, 60000L, "delete", 1));
        topicos.eliminar(nombre);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(topicos.listar()).doesNotContain(nombre));
    }

    @Test
    void administracionRequiereEmpleado() throws Exception {
        try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            var uri = URI.create("http://127.0.0.1:" + puerto + "/api/kafka/topicos");
            assertThat(http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.discarding())
                    .statusCode()).isEqualTo(401);
            assertThat(http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer cliente").build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(403);
            var respuesta = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer empleado").build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(respuesta.statusCode()).isEqualTo(200);
            assertThat(respuesta.body()).contains(SOLICITUDES, CUENTAS);
        }
    }

    private KafkaConsumer<String, String> consumidor(String grupo) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, grupo,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000));
    }

    static class AnaliticaConFalloTemporal extends AnaliticaKafka {

        private final AtomicInteger intentos = new AtomicInteger();

        AnaliticaConFalloTemporal(JdbcClient jdbc) {
            super(jdbc);
        }

        @Override
        public void registrarSolicitud(EventosKafka.SolicitudCreada evento) {
            if ("Cuenta Prueba Reintentos".equals(evento.tipoCuenta()) && intentos.incrementAndGet() <= 2) {
                throw new TransientDataAccessResourceException("Fallo transitorio simulado");
            }
            super.registrarSolicitud(evento);
        }

        int intentosDePrueba() {
            return intentos.get();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ConfiguracionDePrueba {

        @Bean
        @Primary
        AnaliticaConFalloTemporal analiticaConFalloTemporal(JdbcClient jdbc) {
            return new AnaliticaConFalloTemporal(jdbc);
        }

        @Bean
        JwtDecoder decoderDePrueba() {
            return token -> Jwt.withTokenValue(token)
                    .header("alg", "RS256")
                    .claim("sub", "cuenta-prueba")
                    .claim("cognito:groups", List.of("empleado".equals(token) ? "Empleado" : "Cliente"))
                    .build();
        }
    }
}
