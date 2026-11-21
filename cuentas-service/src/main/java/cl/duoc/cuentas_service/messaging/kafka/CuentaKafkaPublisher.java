package cl.duoc.cuentas_service.messaging.kafka;

import cl.duoc.cuentas_service.entity.Cuenta;
import cl.duoc.cuentas_service.entity.CuentaKafkaPendiente;
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
public class CuentaKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(CuentaKafkaPublisher.class);

    private final KafkaTemplate<String, CuentaHabilitadaKafka> kafka;
    private final TransactionTemplate transactionTemplate;
    private final boolean enabled;
    private final String topic;

    @PersistenceContext
    private EntityManager entityManager;

    public CuentaKafkaPublisher(
            KafkaTemplate<String, CuentaHabilitadaKafka> kafka,
            PlatformTransactionManager transactionManager,
            @Value("${app.kafka.cuentas.enabled}") boolean enabled,
            @Value("${app.kafka.cuentas.topic}") String topic) {
        this.kafka = kafka;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.enabled = enabled;
        this.topic = topic;
    }

    public void registrar(Cuenta cuenta) {
        if (enabled) {
            entityManager.persist(
                    new CuentaKafkaPendiente(cuenta.getId(), cuenta.getTipoCuenta()));
        }
    }

    @Scheduled(fixedDelayString = "${app.kafka.cuentas.poll-interval}")
    public void publicarPendiente() {
        if (enabled) {
            transactionTemplate.executeWithoutResult(status -> enviarPrimerPendiente());
        }
    }

    private void enviarPrimerPendiente() {
        var pendientes = entityManager.createQuery(
                        "select p from CuentaKafkaPendiente p order by p.id",
                        CuentaKafkaPendiente.class)
                .setMaxResults(1)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultList();

        if (pendientes.isEmpty()) {
            return;
        }

        var pendiente = pendientes.getFirst();
        var evento = new CuentaHabilitadaKafka(
                pendiente.getEventId(),
                pendiente.getCuentaId(),
                pendiente.getTipoCuenta(),
                pendiente.getOcurridoEn());

        try {
            var resultado = kafka.send(topic, evento.cuentaId().toString(), evento)
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

    public record CuentaHabilitadaKafka(
            UUID eventId,
            Long cuentaId,
            String tipoCuenta,
            Instant ocurridoEn) {
    }
}