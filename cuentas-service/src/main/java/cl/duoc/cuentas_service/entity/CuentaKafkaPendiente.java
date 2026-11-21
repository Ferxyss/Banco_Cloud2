package cl.duoc.cuentas_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "cuentas_kafka_outbox")
public class CuentaKafkaPendiente {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private Long cuentaId;

    @Column(nullable = false)
    private String tipoCuenta;

    @Column(nullable = false)
    private Instant ocurridoEn;

    protected CuentaKafkaPendiente() {
    }

    public CuentaKafkaPendiente(Long cuentaId, String tipoCuenta) {
        this.eventId = UUID.randomUUID();
        this.cuentaId = cuentaId;
        this.tipoCuenta = tipoCuenta;
        this.ocurridoEn = Instant.now();
    }

    public UUID getEventId() {
        return eventId;
    }

    public Long getCuentaId() {
        return cuentaId;
    }

    public String getTipoCuenta() {
        return tipoCuenta;
    }

    public Instant getOcurridoEn() {
        return ocurridoEn;
    }
}