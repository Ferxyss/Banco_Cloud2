package cl.duoc.kafka_service.eventos;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.UUID;

public final class EventosKafka {

    private EventosKafka() {
    }

    public record SolicitudCreada(
            @NotNull UUID eventId,
            @NotNull @Positive Long solicitudId,
            @NotBlank @Size(max = 255) String tipoCuenta,
            @NotNull Instant ocurridoEn) {
    }

    public record CuentaHabilitada(
            @NotNull UUID eventId,
            @NotNull @Positive Long cuentaId,
            @NotBlank @Size(max = 255) String tipoCuenta,
            @NotNull Instant ocurridoEn) {
    }
}
