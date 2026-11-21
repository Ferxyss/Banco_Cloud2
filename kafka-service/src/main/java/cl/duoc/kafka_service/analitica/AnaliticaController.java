package cl.duoc.kafka_service.analitica;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/kafka/metricas")
public class AnaliticaController {

    private final AnaliticaKafka analitica;

    public AnaliticaController(AnaliticaKafka analitica) {
        this.analitica = analitica;
    }

    @GetMapping
    public List<AnaliticaKafka.Resumen> resumen() {
        return analitica.resumen();
    }
}
