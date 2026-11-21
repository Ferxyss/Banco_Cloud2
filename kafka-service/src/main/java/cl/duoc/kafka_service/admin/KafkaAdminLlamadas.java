package cl.duoc.kafka_service.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

final class KafkaAdminLlamadas {

    private KafkaAdminLlamadas() {
    }

    static <T> T esperar(KafkaFuture<T> resultado) {
        try {
            return resultado.get(9, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new TimeoutException("La operación Kafka fue interrumpida", error);
        } catch (java.util.concurrent.TimeoutException error) {
            throw new TimeoutException("Kafka no respondió dentro del plazo", error);
        } catch (ExecutionException error) {
            if (error.getCause() instanceof RuntimeException causa) {
                throw causa;
            }
            throw new IllegalStateException("No se pudo completar la operación Kafka", error.getCause());
        }
    }
}
