package cl.duoc.cuentas_service.repository;

import cl.duoc.cuentas_service.entity.Cuenta;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CuentaRepository extends JpaRepository<Cuenta, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Cuenta> findForUpdateById(Long id);
}