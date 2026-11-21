package cl.duoc.kafka_service.admin;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static cl.duoc.kafka_service.admin.KafkaAdminLlamadas.esperar;

@Service
public class KafkaGrupos {

    private static final Comparator<TopicPartition> ORDEN_PARTICIONES =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private final Admin admin;

    public KafkaGrupos(Admin admin) {
        this.admin = admin;
    }

    public List<GrupoResumen> listar() {
        return esperar(admin.listGroups().all()).stream()
                .filter(g -> g.type().filter(t -> t == GroupType.SHARE || t == GroupType.STREAMS).isEmpty())
                .map(g -> new GrupoResumen(g.groupId(), g.groupState().map(Object::toString).orElse("Unknown")))
                .sorted(Comparator.comparing(GrupoResumen::id))
                .toList();
    }

    public DetalleGrupo inspeccionar(String id) {
        var descripcion = describir(id);
        var commits = esperar(admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata());

        var particiones = new TreeSet<>(ORDEN_PARTICIONES);
        particiones.addAll(commits.keySet());
        var miembros = descripcion.members().stream().map(m -> {
            var asignadas = m.assignment().topicPartitions().stream().sorted(ORDEN_PARTICIONES).toList();
            particiones.addAll(asignadas);
            return new Miembro(m.consumerId(), m.clientId(), asignadas.stream()
                    .map(p -> new Particion(p.topic(), p.partition())).toList());
        }).toList();

        var consulta = new LinkedHashMap<TopicPartition, OffsetSpec>();
        particiones.forEach(p -> consulta.put(p, OffsetSpec.latest()));
        var finales = consulta.isEmpty() ? Map.<TopicPartition, Long>of() : offsets(consulta);
        var progreso = particiones.stream().map(p -> {
            var commit = commits.get(p);
            Long confirmado = commit == null ? null : commit.offset();
            long fin = finales.get(p);
            Long lag = confirmado == null ? null : Math.max(0, fin - confirmado);
            return new Progreso(p.topic(), p.partition(), confirmado, fin, lag);
        }).toList();

        return new DetalleGrupo(id, descripcion.groupState().toString(), miembros, progreso);
    }

    public DetalleGrupo restablecerOffsets(String id, Restablecer solicitud) {
        if (!describir(id).members().isEmpty()) {
            throw new GroupNotEmptyException("Detén los consumidores del grupo antes de cambiar sus offsets");
        }

        var topico = esperar(admin.describeTopics(List.of(solicitud.topico()))
                .topicNameValues().get(solicitud.topico()));
        var consulta = new LinkedHashMap<TopicPartition, OffsetSpec>();
        var posicion = solicitud.posicion() == Posicion.INICIO ? OffsetSpec.earliest() : OffsetSpec.latest();
        topico.partitions().forEach(p ->
                consulta.put(new TopicPartition(solicitud.topico(), p.partition()), posicion));

        var cambios = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
        offsets(consulta).forEach((p, offset) -> cambios.put(p, new OffsetAndMetadata(offset)));
        esperar(admin.alterConsumerGroupOffsets(id, cambios).all());
        return inspeccionar(id);
    }

    public void eliminar(String id) {
        esperar(admin.deleteConsumerGroups(List.of(id)).all());
    }

    private ConsumerGroupDescription describir(String id) {
        return esperar(admin.describeConsumerGroups(List.of(id)).describedGroups().get(id));
    }

    private Map<TopicPartition, Long> offsets(Map<TopicPartition, OffsetSpec> consulta) {
        var resultado = new LinkedHashMap<TopicPartition, Long>();
        esperar(admin.listOffsets(consulta).all()).forEach((p, info) -> resultado.put(p, info.offset()));
        return resultado;
    }

    public enum Posicion {
        INICIO, FIN
    }

    public record Restablecer(@NotBlank String topico, @NotNull Posicion posicion) {
    }

    public record GrupoResumen(String id, String estado) {
    }

    public record DetalleGrupo(String id, String estado, List<Miembro> miembros, List<Progreso> progreso) {
    }

    public record Miembro(String id, String cliente, List<Particion> asignaciones) {
    }

    public record Particion(String topico, int particion) {
    }

    public record Progreso(String topico, int particion, Long offsetConfirmado, long offsetFinal, Long lag) {
    }
}
