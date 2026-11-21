package cl.duoc.kafka_service.admin;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaTopicos topicos;
    private final KafkaGrupos grupos;

    public KafkaAdminController(KafkaTopicos topicos, KafkaGrupos grupos) {
        this.topicos = topicos;
        this.grupos = grupos;
    }

    @GetMapping("/topicos")
    public List<String> listarTopicos() {
        return topicos.listar();
    }

    @PostMapping("/topicos")
    @ResponseStatus(HttpStatus.CREATED)
    public KafkaTopicos.Detalle crearTopico(@Valid @RequestBody KafkaTopicos.Crear solicitud) {
        return topicos.crear(solicitud);
    }

    @GetMapping("/topicos/{nombre}")
    public KafkaTopicos.Detalle consultarTopico(@PathVariable String nombre) {
        return topicos.inspeccionar(nombre);
    }

    @DeleteMapping("/topicos/{nombre}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminarTopico(@PathVariable String nombre) {
        topicos.eliminar(nombre);
    }

    @PutMapping("/topicos/{nombre}/particiones")
    public KafkaTopicos.Detalle aumentarParticiones(@PathVariable String nombre,
            @Valid @RequestBody KafkaTopicos.CambiarParticiones solicitud) {
        return topicos.aumentarParticiones(nombre, solicitud.particiones());
    }

    @PatchMapping("/topicos/{nombre}/configuracion")
    public KafkaTopicos.Detalle configurar(@PathVariable String nombre,
            @Valid @RequestBody KafkaTopicos.Configurar solicitud) {
        return topicos.configurar(nombre, solicitud);
    }

    @DeleteMapping("/topicos/{nombre}/configuracion")
    public KafkaTopicos.Detalle restablecerConfiguracion(@PathVariable String nombre, @RequestParam String clave) {
        return topicos.restablecerConfiguracion(nombre, clave);
    }

    @GetMapping("/grupos")
    public List<KafkaGrupos.GrupoResumen> listarGrupos() {
        return grupos.listar();
    }

    @GetMapping("/grupos/{id}")
    public KafkaGrupos.DetalleGrupo consultarGrupo(@PathVariable String id) {
        return grupos.inspeccionar(id);
    }

    @PutMapping("/grupos/{id}/offsets")
    public KafkaGrupos.DetalleGrupo restablecerOffsets(@PathVariable String id,
            @Valid @RequestBody KafkaGrupos.Restablecer solicitud) {
        return grupos.restablecerOffsets(id, solicitud);
    }

    @DeleteMapping("/grupos/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminarGrupo(@PathVariable String id) {
        grupos.eliminar(id);
    }
}
