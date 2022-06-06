package br.com.producer.controller;

import br.com.avro.CarAvro;
import br.com.producer.controller.dto.CarDTO;
import br.com.producer.producer.CarProducer;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.UUID;

@Controller
@RequestMapping("/car")
@AllArgsConstructor
public class CarController {

    private final CarProducer carProducer;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> sendMessage(@RequestBody CarDTO carDTO) {
        var id = UUID.randomUUID().toString();

        var carAvro = CarAvro.newBuilder()
                .setId(id)
                .setName(carDTO.getName())
                .setBrand(carDTO.getBrand())
                .build();

        carProducer.sendMessage(carAvro);

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

}
