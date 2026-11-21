package cl.duoc.solicitudes_service.messaging.kafka;

import cl.duoc.solicitudes_service.entity.Solicitud;
import cl.duoc.solicitudes_service.entity.SolicitudKafkaPendiente;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class SolicitudKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(SolicitudKafkaPublisher.class);

    private final KafkaTemplate<String, SolicitudCreadaKafka> kafka;
    private final TransactionTemplate transactionTemplate;
    private final boolean enabled;
    private final String topic;

    @PersistenceContext
    private EntityManager entityManager;

    public SolicitudKafkaPublisher(
            KafkaTemplate<String, SolicitudCreadaKafka> kafka,
            PlatformTransactionManager transactionManager,
            @Value("${app.kafka.solicitudes.enabled}") boolean enabled,
            @Value("${app.kafka.solicitudes.topic}") String topic) {
        this.kafka = kafka;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.enabled = enabled;
        this.topic = topic;
    }

    public void registrar(Solicitud solicitud) {
        if (enabled) {
            entityManager.persist(
                    new SolicitudKafkaPendiente(solicitud.getId(), solicitud.getTipoCuenta()));
        }
    }

    @Scheduled(fixedDelayString = "${app.kafka.solicitudes.poll-interval}")
    public void publicarPendiente() {
        if (enabled) {
            transactionTemplate.executeWithoutResult(status -> enviarPrimerPendiente());
        }
    }

    private void enviarPrimerPendiente() {
        var pendientes = entityManager.createQuery(
                        "select p from SolicitudKafkaPendiente p order by p.id",
                        SolicitudKafkaPendiente.class)
                .setMaxResults(1)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultList();

        if (pendientes.isEmpty()) {
            return;
        }

        var pendiente = pendientes.getFirst();
        var evento = new SolicitudCreadaKafka(
                pendiente.getEventId(),
                pendiente.getSolicitudId(),
                pendiente.getTipoCuenta(),
                pendiente.getOcurridoEn());

        try {
            var resultado = kafka.send(topic, evento.solicitudId().toString(), evento)
                    .get(12, TimeUnit.SECONDS);

            entityManager.remove(pendiente);

            var metadata = resultado.getRecordMetadata();
            log.info("Publicación Kafka confirmada eventId={} topic={} partition={} offset={}",
                    evento.eventId(), topic, metadata.partition(), metadata.offset());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            log.warn("Publicación Kafka interrumpida; pendiente eventId={} topic={}",
                    evento.eventId(), topic);
        } catch (KafkaException | ExecutionException | TimeoutException error) {
            log.warn("Publicación Kafka pendiente de reintento eventId={} topic={}: {}",
                    evento.eventId(), topic, error.toString());
        }
    }

    public record SolicitudCreadaKafka(
            UUID eventId,
            Long solicitudId,
            String tipoCuenta,
            Instant ocurridoEn) {
    }
}
