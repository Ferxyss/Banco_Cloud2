package cl.duoc.kafka_service.admin;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cl.duoc.kafka_service.admin.KafkaAdminLlamadas.esperar;

@Service
public class KafkaTopicos {

    private static final List<String> CONFIGS = List.of(
            TopicConfig.RETENTION_MS_CONFIG, TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, TopicConfig.RETENTION_BYTES_CONFIG);
    private static final String CLEANUP_REGEX = "delete|compact|compact,delete|delete,compact";

    private final Admin admin;

    public KafkaTopicos(Admin admin) {
        this.admin = admin;
    }

    public List<String> listar() {
        return esperar(admin.listTopics().names()).stream().sorted().toList();
    }

    public Detalle inspeccionar(String nombre) {
        var descripcion = describir(nombre);
        var recurso = recurso(nombre);
        var config = esperar(admin.describeConfigs(List.of(recurso)).all()).get(recurso);
        var configuraciones = new LinkedHashMap<String, Configuracion>();
        for (String clave : CONFIGS) {
            var entrada = config.get(clave);
            configuraciones.put(clave, new Configuracion(entrada.value(), entrada.source().name()));
        }

        var particiones = descripcion.partitions().stream()
                .map(p -> new Particion(p.partition(), Optional.ofNullable(p.leader()).map(Node::id).orElse(null),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();
        return new Detalle(descripcion.name(), configuraciones, particiones);
    }

    public Detalle crear(Crear solicitud) {
        var configs = configuraciones(solicitud.retencionMs(), solicitud.cleanupPolicy(), solicitud.minIsr());
        if (solicitud.minIsr() != null && solicitud.minIsr() > solicitud.replicas()) {
            throw new IllegalArgumentException("minIsr debe ser menor o igual al factor de replicación");
        }
        var topico = new NewTopic(solicitud.nombre(), solicitud.particiones(), solicitud.replicas()).configs(configs);
        esperar(admin.createTopics(List.of(topico)).all());
        return inspeccionar(solicitud.nombre());
    }

    public void eliminar(String nombre) {
        esperar(admin.deleteTopics(List.of(nombre)).all());
    }

    public Detalle aumentarParticiones(String nombre, int total) {
        int actual = describir(nombre).partitions().size();
        if (total < actual) {
            throw new IllegalArgumentException("Kafka no permite disminuir la cantidad de particiones");
        }
        if (total > actual) {
            esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all());
        }
        return inspeccionar(nombre);
    }

    public Detalle configurar(String nombre, Configurar solicitud) {
        var configs = configuraciones(solicitud.retencionMs(), solicitud.cleanupPolicy(), solicitud.minIsr());
        if (configs.isEmpty() && solicitud.retencionBytes() == null) {
            throw new IllegalArgumentException("Indica al menos una configuración");
        }

        if (solicitud.retencionBytes() != null) {
            configs.put(TopicConfig.RETENTION_BYTES_CONFIG, solicitud.retencionBytes().toString());
        }
        if (solicitud.minIsr() != null) {
            int replicasMinimas = describir(nombre).partitions().stream()
                    .mapToInt(p -> p.replicas().size()).min().orElseThrow();
            if (solicitud.minIsr() > replicasMinimas) {
                throw new IllegalArgumentException("minIsr debe ser menor o igual al factor de replicación");
            }
        }

        var cambios = configs.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        esperar(admin.incrementalAlterConfigs(Map.of(recurso(nombre), cambios)).all());
        return inspeccionar(nombre);
    }

    public Detalle restablecerConfiguracion(String nombre, String clave) {
        if (!CONFIGS.contains(clave)) {
            throw new IllegalArgumentException("Usa una configuración básica admitida: " + CONFIGS);
        }
        var cambio = new AlterConfigOp(new ConfigEntry(clave, null), AlterConfigOp.OpType.DELETE);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso(nombre), List.of(cambio))).all());
        return inspeccionar(nombre);
    }

    private TopicDescription describir(String nombre) {
        return esperar(admin.describeTopics(List.of(nombre)).topicNameValues().get(nombre));
    }

    private ConfigResource recurso(String nombre) {
        return new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    }

    private Map<String, String> configuraciones(Long retencion, String politica, Integer minIsr) {
        var config = new LinkedHashMap<String, String>();
        if (retencion != null) {
            config.put(TopicConfig.RETENTION_MS_CONFIG, retencion.toString());
        }
        if (politica != null) {
            config.put(TopicConfig.CLEANUP_POLICY_CONFIG, politica);
        }
        if (minIsr != null) {
            config.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr.toString());
        }
        return config;
    }

    public record Crear(
            @NotBlank @Size(max = 249) String nombre,
            @Min(1) int particiones,
            @Min(1) short replicas,
            @Min(-1) Long retencionMs,
            @Pattern(regexp = CLEANUP_REGEX) String cleanupPolicy,
            @Min(1) Integer minIsr) {
    }

    public record Configurar(
            @Min(-1) Long retencionMs,
            @Pattern(regexp = CLEANUP_REGEX) String cleanupPolicy,
            @Min(1) Integer minIsr,
            @Min(-1) Long retencionBytes) {
    }

    public record CambiarParticiones(@Min(1) int particiones) {
    }

    public record Detalle(String nombre, Map<String, Configuracion> configuraciones, List<Particion> particiones) {
    }

    public record Configuracion(String valor, String fuente) {
    }

    public record Particion(int numero, Integer lider, List<Integer> replicas, List<Integer> sincronizadas) {
    }
}
