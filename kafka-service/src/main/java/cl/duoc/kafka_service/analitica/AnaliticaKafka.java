package cl.duoc.kafka_service.analitica;

import cl.duoc.kafka_service.eventos.EventosKafka;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Service
public class AnaliticaKafka {

    private final JdbcClient jdbc;

    public AnaliticaKafka(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void registrarSolicitud(EventosKafka.SolicitudCreada evento) {
        guardar(evento.eventId(), "SOLICITUD_CREADA", evento.solicitudId(), evento.tipoCuenta(), evento.ocurridoEn());
    }

    public void registrarCuenta(EventosKafka.CuentaHabilitada evento) {
        guardar(evento.eventId(), "CUENTA_HABILITADA", evento.cuentaId(), evento.tipoCuenta(), evento.ocurridoEn());
    }

    private void guardar(UUID id, String flujo, Long entidadId, String tipoCuenta, Instant ocurridoEn) {
        // Kafka puede reenviar un evento si falla el commit del offset después de guardar en MySQL.
        jdbc.sql("""
                INSERT INTO kafka_eventos_procesados (event_id, flujo, entidad_id, tipo_cuenta, ocurrido_en)
                VALUES (:eventId, :flujo, :entidadId, :tipoCuenta, :ocurridoEn)
                ON DUPLICATE KEY UPDATE event_id = event_id
                """)
                .param("eventId", id.toString())
                .param("flujo", flujo)
                .param("entidadId", entidadId)
                .param("tipoCuenta", tipoCuenta)
                .param("ocurridoEn", LocalDateTime.ofInstant(ocurridoEn, ZoneOffset.UTC))
                .update();
    }

    public List<Resumen> resumen() {
        return jdbc.sql("""
                SELECT flujo, tipo_cuenta, COUNT(*) AS eventos
                FROM kafka_eventos_procesados
                GROUP BY flujo, tipo_cuenta
                ORDER BY flujo, tipo_cuenta
                """)
                .query((rs, fila) -> new Resumen(rs.getString("flujo"), rs.getString("tipo_cuenta"),
                        rs.getLong("eventos")))
                .list();
    }

    public record Resumen(String flujo, String tipoCuenta, long eventos) {
    }
}
