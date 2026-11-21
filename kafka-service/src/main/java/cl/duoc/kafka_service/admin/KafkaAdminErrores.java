package cl.duoc.kafka_service.admin;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = KafkaAdminController.class)
public class KafkaAdminErrores {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminErrores.class);

    @ExceptionHandler({UnknownTopicOrPartitionException.class, GroupIdNotFoundException.class})
    ProblemDetail noExiste(RuntimeException error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, error.getMessage());
    }

    @ExceptionHandler({TopicExistsException.class, GroupNotEmptyException.class, GroupSubscribedToTopicException.class})
    ProblemDetail conflicto(RuntimeException error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, error.getMessage());
    }

    @ExceptionHandler({InvalidConfigurationException.class, InvalidTopicException.class, InvalidPartitionsException.class,
            InvalidReplicationFactorException.class, InvalidRequestException.class, PolicyViolationException.class,
            IllegalArgumentException.class})
    ProblemDetail solicitudInvalida(RuntimeException error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, error.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail camposInvalidos(MethodArgumentNotValidException error) {
        String detalle = error.getBindingResult().getFieldErrors().stream()
                .map(campo -> campo.getField() + ": " + campo.getDefaultMessage())
                .distinct().collect(java.util.stream.Collectors.joining(", "));
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detalle);
    }

    @ExceptionHandler(AuthorizationException.class)
    ProblemDetail sinPermisos() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.FORBIDDEN, "Kafka rechazó la autorización de la operación");
    }

    @ExceptionHandler(RetriableException.class)
    ProblemDetail kafkaOcupado() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Kafka no pudo completar la operación en este momento");
    }

    @ExceptionHandler(KafkaException.class)
    ProblemDetail errorKafka(KafkaException error) {
        log.warn("Falló una operación de administración de Kafka", error);
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, "Kafka rechazó la operación");
    }
}
