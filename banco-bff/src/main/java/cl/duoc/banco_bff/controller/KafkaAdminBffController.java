package cl.duoc.banco_bff.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminBffController {

    private final RestClient restClient;
    private final String kafkaServiceUrl;

    public KafkaAdminBffController(RestClient restClient, @Value("${kafka-service.url}") String kafkaServiceUrl) {
        this.restClient = restClient;
        this.kafkaServiceUrl = kafkaServiceUrl;
    }

    @GetMapping("/metricas")
    public ResponseEntity<byte[]> metricas() {
        return enviar(HttpMethod.GET, "/metricas", null);
    }

    @GetMapping("/topicos")
    public ResponseEntity<byte[]> topicos() {
        return enviar(HttpMethod.GET, "/topicos", null);
    }

    @GetMapping("/topicos/{nombre}")
    public ResponseEntity<byte[]> topico(@PathVariable String nombre) {
        return enviar(HttpMethod.GET, "/topicos/{nombre}", null, nombre);
    }

    @PostMapping("/topicos")
    public ResponseEntity<byte[]> crearTopico(@RequestBody Map<String, Object> solicitud) {
        return enviar(HttpMethod.POST, "/topicos", solicitud);
    }

    @PutMapping("/topicos/{nombre}/retencion")
    public ResponseEntity<byte[]> cambiarRetencion(@PathVariable String nombre,
            @Valid @RequestBody CambiarRetencion solicitud) {
        return enviar(HttpMethod.PATCH, "/topicos/{nombre}/configuracion", solicitud, nombre);
    }

    @DeleteMapping("/topicos/{nombre}")
    public ResponseEntity<byte[]> eliminarTopico(@PathVariable String nombre) {
        return enviar(HttpMethod.DELETE, "/topicos/{nombre}", null, nombre);
    }

    @GetMapping("/grupos")
    public ResponseEntity<byte[]> grupos() {
        return enviar(HttpMethod.GET, "/grupos", null);
    }

    @GetMapping("/grupos/{id}")
    public ResponseEntity<byte[]> grupo(@PathVariable String id) {
        return enviar(HttpMethod.GET, "/grupos/{id}", null, id);
    }

    private ResponseEntity<byte[]> enviar(HttpMethod metodo, String ruta, Object cuerpo, Object... variables) {
        var uri = UriComponentsBuilder.fromUriString(kafkaServiceUrl)
                .path("/api/kafka")
                .path(ruta)
                .encode()
                .buildAndExpand(variables)
                .toUri();
        var solicitud = restClient.method(metodo).uri(uri);
        if (cuerpo != null) {
            solicitud.contentType(MediaType.APPLICATION_JSON).body(cuerpo);
        }

        return solicitud.exchange((request, respuesta) -> {
            var salida = ResponseEntity.status(respuesta.getStatusCode());
            var tipo = respuesta.getHeaders().getContentType();
            if (tipo != null) {
                salida.contentType(tipo);
            }
            byte[] contenido = respuesta.getBody().readAllBytes();
            return salida.body(contenido.length == 0 ? null : contenido);
        });
    }

    @ExceptionHandler(ResourceAccessException.class)
    ProblemDetail servicioNoDisponible() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no está disponible en este momento");
    }

    public record CambiarRetencion(@NotNull @Min(-1) Long retencionMs) {
    }
}
