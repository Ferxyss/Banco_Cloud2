package cl.duoc.solicitudes_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "solicitudes_kafka_outbox")
public class SolicitudKafkaPendiente {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private Long solicitudId;

    @Column(nullable = false)
    private String tipoCuenta;

    @Column(nullable = false)
    private Instant ocurridoEn;

    protected SolicitudKafkaPendiente() {
    }

    public SolicitudKafkaPendiente(Long solicitudId, String tipoCuenta) {
        this.eventId = UUID.randomUUID();
        this.solicitudId = solicitudId;
        this.tipoCuenta = tipoCuenta;
        this.ocurridoEn = Instant.now();
    }

    public UUID getEventId() {
        return eventId;
    }

    public Long getSolicitudId() {
        return solicitudId;
    }

    public String getTipoCuenta() {
        return tipoCuenta;
    }

    public Instant getOcurridoEn() {
        return ocurridoEn;
    }
}
