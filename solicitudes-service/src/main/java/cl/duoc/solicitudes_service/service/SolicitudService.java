package cl.duoc.solicitudes_service.service;

import cl.duoc.solicitudes_service.entity.Solicitud;
import cl.duoc.solicitudes_service.messaging.kafka.SolicitudKafkaPublisher;
import cl.duoc.solicitudes_service.messaging.solicitudes.SolicitudEvent;
import cl.duoc.solicitudes_service.messaging.solicitudes.SolicitudEventPublisher;
import cl.duoc.solicitudes_service.repository.SolicitudRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class SolicitudService {

    private final SolicitudRepository solicitudRepository;
    private final SolicitudEventPublisher eventPublisher;
    private final SolicitudKafkaPublisher kafkaPublisher;
    private final TransactionTemplate transactionTemplate;

    public SolicitudService(
            SolicitudRepository solicitudRepository,
            SolicitudEventPublisher eventPublisher,
            SolicitudKafkaPublisher kafkaPublisher,
            PlatformTransactionManager transactionManager) {
        this.solicitudRepository = solicitudRepository;
        this.eventPublisher = eventPublisher;
        this.kafkaPublisher = kafkaPublisher;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public Solicitud crearSolicitud(Solicitud solicitud) {
        solicitud.setEstado("PENDIENTE");
        solicitud.setFechaSolicitud(LocalDateTime.now());

        Solicitud guardada = transactionTemplate.execute(status -> {
            Solicitud persistida = solicitudRepository.save(solicitud);
            kafkaPublisher.registrar(persistida);
            return persistida;
        });

        SolicitudEvent evento = new SolicitudEvent(
                guardada.getId(),
                guardada.getTipoCuenta(),
                guardada.getMontoInicial(),
                guardada.getCliente(),
                guardada.getEstado(),
                guardada.getFechaSolicitud()
        );

        eventPublisher.publicarSolicitudCreada(evento);

        return guardada;
    }

    public List<Solicitud> listarSolicitudes() {
        return solicitudRepository.findAll();
    }

    public Optional<Solicitud> buscarPorId(Long id) {
        return solicitudRepository.findById(id);
    }

    public Solicitud aprobarSolicitud(Long id) {
        Solicitud solicitud = solicitudRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Solicitud no encontrada"));

        solicitud.setEstado("APROBADA");

        return solicitudRepository.save(solicitud);
    }

    public Solicitud rechazarSolicitud(Long id) {
        Solicitud solicitud = solicitudRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Solicitud no encontrada"));

        solicitud.setEstado("RECHAZADA");

        return solicitudRepository.save(solicitud);
    }
}