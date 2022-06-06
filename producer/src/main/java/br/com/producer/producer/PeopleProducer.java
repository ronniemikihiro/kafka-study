package br.com.producer.producer;

import br.com.avro.PeopleAvro;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PeopleProducer {

    private final String topicName;
    private final KafkaTemplate<String, PeopleAvro> kafkaTemplate;

    public PeopleProducer(@Value("${topic.name}") String topicName, KafkaTemplate<String, PeopleAvro> kafkaTemplate) {
        this.topicName = topicName;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(PeopleAvro peopleAvro) {
        kafkaTemplate.send(topicName, peopleAvro.getId().toString(), peopleAvro).addCallback(
                success -> log.info("Mensagem enviada com sucesso: {}", peopleAvro),
                failure -> log.error("Falha ao enviar a mensagem: {}", peopleAvro)
        );
    }
}
