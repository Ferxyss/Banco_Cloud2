package cl.duoc.kafka_service.config;

import cl.duoc.kafka_service.eventos.EventoInvalidoException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
public class KafkaInfraestructura {

    private static final Logger log = LoggerFactory.getLogger(KafkaInfraestructura.class);
    private static final String SUFIJO_DLQ = ".DLT";

    @Bean(destroyMethod = "close")
    Admin adminKafka(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    KafkaAdmin.NewTopics topicosFallidos(
            @Value("${app.kafka.topicos.solicitudes}") String solicitudes,
            @Value("${app.kafka.topicos.cuentas}") String cuentas,
            @Value("${app.kafka.dlq.particiones}") int particiones,
            @Value("${app.kafka.dlq.replicas}") int replicas,
            @Value("${app.kafka.dlq.min-isr}") int minIsr,
            @Value("${app.kafka.dlq.retencion-ms}") long retencionMs) {
        return new KafkaAdmin.NewTopics(
                crearDlq(solicitudes, particiones, replicas, minIsr, retencionMs),
                crearDlq(cuentas, particiones, replicas, minIsr, retencionMs));
    }

    private NewTopic crearDlq(String origen, int particiones, int replicas, int minIsr, long retencionMs) {
        return TopicBuilder.name(origen + SUFIJO_DLQ)
                .partitions(particiones)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retencionMs))
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsr))
                .build();
    }

    @Bean
    DefaultErrorHandler erroresKafka(KafkaTemplate<String, String> kafka) {
        // El broker selecciona la partición del tópico de fallos, incluso si crece el tópico de origen.
        var fallidos = new DeadLetterPublishingRecoverer(kafka,
                (registro, error) -> new TopicPartition(registro.topic() + SUFIJO_DLQ, -1));
        fallidos.setFailIfSendResultIsError(true);

        var errores = new DefaultErrorHandler(fallidos, new FixedBackOff(1000, 2));
        errores.addNotRetryableExceptions(EventoInvalidoException.class);
        errores.setRetryListeners((registro, error, intento) ->
                log.warn("Fallo al consumir Kafka topic={} partition={} offset={} intento={}: {}",
                        registro.topic(), registro.partition(), registro.offset(), intento, error.getMessage()));
        return errores;
    }
}
