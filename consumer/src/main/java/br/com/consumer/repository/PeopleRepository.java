package br.com.consumer.repository;

import br.com.consumer.domain.People;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PeopleRepository extends JpaRepository<People, String> {
}
