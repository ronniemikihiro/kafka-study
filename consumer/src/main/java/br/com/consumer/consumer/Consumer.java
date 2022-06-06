package br.com.consumer.consumer;

import br.com.avro.CarAvro;
import br.com.avro.PeopleAvro;
import br.com.consumer.domain.Book;
import br.com.consumer.domain.Car;
import br.com.consumer.domain.People;
import br.com.consumer.repository.CarRepository;
import br.com.consumer.repository.PeopleRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
@KafkaListener(topics = "${topic.name}")
public class Consumer {

    private final PeopleRepository peopleRepository;
    private final CarRepository carRepository;

    @KafkaHandler
    public void consumerPeople(ConsumerRecord<String, PeopleAvro> consumerRecord, Acknowledgment ack) {
        var peopleAvro = consumerRecord.value();

        log.info("Mensagem recebida com sucesso (People): {}", peopleAvro);

        var people = People.builder()
                        .id(peopleAvro.getId().toString())
                        .name(peopleAvro.getName().toString())
                        .cpf(peopleAvro.getCpf().toString())
                        .build();

        people.setBooks(peopleAvro.getBooks().stream()
                .map(book -> Book.builder()
                        .name(book.toString())
                        .people(people)
                        .build())
                        .toList());

        peopleRepository.save(people);

        ack.acknowledge();
    }

    @KafkaHandler
    public void consumerCar(ConsumerRecord<String, CarAvro> consumerRecord, Acknowledgment ack) {
        var carAvro = consumerRecord.value();

        log.info("Mensagem recebida com sucesso (Car): {}", carAvro);

        var people = Car.builder()
                .id(carAvro.getId().toString())
                .name(carAvro.getName().toString())
                .brand(carAvro.getBrand().toString())
                .build();

        carRepository.save(people);

        ack.acknowledge();
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object, Acknowledgment ack) {
        log.info("Mensagem recebida com sucesso (Object): {}", object);
        ack.acknowledge();
    }

}
