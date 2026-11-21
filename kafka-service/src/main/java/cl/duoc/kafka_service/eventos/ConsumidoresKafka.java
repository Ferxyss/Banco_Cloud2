package cl.duoc.kafka_service.eventos;

import cl.duoc.kafka_service.analitica.AnaliticaKafka;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Component
public class ConsumidoresKafka {

    private static final Logger log = LoggerFactory.getLogger(ConsumidoresKafka.class);

    private final JsonMapper json;
    private final Validator validator;
    private final AnaliticaKafka analitica;

    public ConsumidoresKafka(JsonMapper json, Validator validator, AnaliticaKafka analitica) {
        this.json = json;
        this.validator = validator;
        this.analitica = analitica;
    }

    @KafkaListener(topics = "${app.kafka.topicos.solicitudes}", groupId = "${app.kafka.grupos.solicitudes}")
    public void recibirSolicitud(ConsumerRecord<String, String> registro) {
        var evento = convertir(registro.value(), EventosKafka.SolicitudCreada.class);
        validarClave(registro, evento.solicitudId());
        analitica.registrarSolicitud(evento);
        registrarConsumo(registro, evento.eventId());
    }

    @KafkaListener(topics = "${app.kafka.topicos.cuentas}", groupId = "${app.kafka.grupos.cuentas}")
    public void recibirCuenta(ConsumerRecord<String, String> registro) {
        var evento = convertir(registro.value(), EventosKafka.CuentaHabilitada.class);
        validarClave(registro, evento.cuentaId());
        analitica.registrarCuenta(evento);
        registrarConsumo(registro, evento.eventId());
    }

    private <T> T convertir(String valor, Class<T> tipo) {
        if (valor == null) {
            throw new EventoInvalidoException("El evento no contiene un payload");
        }

        T evento;
        try {
            evento = json.readValue(valor, tipo);
        } catch (JacksonException ex) {
            throw new EventoInvalidoException("El payload no cumple el contrato JSON del tópico", ex);
        }

        if (evento == null || !validator.validate(evento).isEmpty()) {
            throw new EventoInvalidoException("El payload contiene campos inválidos");
        }
        return evento;
    }

    private void validarClave(ConsumerRecord<String, String> registro, Long id) {
        if (!id.toString().equals(registro.key())) {
            throw new EventoInvalidoException("La clave Kafka debe coincidir con el identificador del evento de negocio");
        }
    }

    private void registrarConsumo(ConsumerRecord<String, String> registro, UUID eventId) {
        log.info("Evento Kafka procesado eventId={} topic={} partition={} offset={}",
                eventId, registro.topic(), registro.partition(), registro.offset());
    }
}
