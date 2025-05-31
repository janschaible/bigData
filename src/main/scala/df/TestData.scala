package df

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.javafaker.Faker

import java.io.File

class Phone(val number: String, val model: String)

class Person(val id: Int, val name: String, val phones: List[Phone])

object TestData {

  def main(args: Array[String]): Unit = {
    val faker = new Faker()
    val people = (0 until 100).toList.map { i =>
      val phones = List.fill(faker.random().nextInt(0, 5)) {
        new Phone(faker.phoneNumber().phoneNumber(), faker.animal().name())
      }
      new Person(i, faker.name().name(), phones)
    }
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(people.take(3)))
    mapper.writeValue(new File("data/testPeople.json"), people)
  }
}
