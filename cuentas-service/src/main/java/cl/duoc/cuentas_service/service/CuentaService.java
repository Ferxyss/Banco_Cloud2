package cl.duoc.cuentas_service.service;

import cl.duoc.cuentas_service.entity.Cuenta;
import cl.duoc.cuentas_service.messaging.kafka.CuentaKafkaPublisher;
import cl.duoc.cuentas_service.repository.CuentaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Optional;

@Service
public class CuentaService {

    private final CuentaRepository cuentaRepository;
    private final CuentaKafkaPublisher kafkaPublisher;

    public CuentaService(
            CuentaRepository cuentaRepository,
            CuentaKafkaPublisher kafkaPublisher) {
        this.cuentaRepository = cuentaRepository;
        this.kafkaPublisher = kafkaPublisher;
    }

    public List<Cuenta> listarCuentas() {
        return cuentaRepository.findAll();
    }

    public Optional<Cuenta> buscarPorId(Long id) {
        return cuentaRepository.findById(id);
    }

    public Cuenta crearCuenta(Cuenta cuenta) {
        return cuentaRepository.save(cuenta);
    }

    @Transactional
    public Cuenta asignarNumeroCuenta(Long id, String numeroCuenta) {
        Cuenta cuenta = cuentaRepository.findForUpdateById(id)
                .orElseThrow(() -> new RuntimeException("Cuenta no encontrada"));

        boolean seHabilita = !StringUtils.hasText(cuenta.getNumeroCuenta())
                && StringUtils.hasText(numeroCuenta);

        cuenta.setNumeroCuenta(numeroCuenta);
        Cuenta guardada = cuentaRepository.save(cuenta);

        if (seHabilita) {
            kafkaPublisher.registrar(guardada);
        }

        return guardada;
    }
}