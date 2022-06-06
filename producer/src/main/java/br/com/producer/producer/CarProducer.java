package br.com.producer.producer;

import br.com.avro.CarAvro;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CarProducer {

    private final String topicName;
    private final KafkaTemplate<String, CarAvro> kafkaTemplate;

    public CarProducer(@Value("${topic.name}") String topicName, KafkaTemplate<String, CarAvro> kafkaTemplate) {
        this.topicName = topicName;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(CarAvro carAvro) {
        kafkaTemplate.send(topicName, carAvro.getId().toString(), carAvro).addCallback(
                success -> log.info("Mensagem enviada com sucesso: {}", carAvro),
                failure -> log.error("Falha ao enviar a mensagem: {}", carAvro)
        );
    }
}
