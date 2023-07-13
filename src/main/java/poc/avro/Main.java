package poc.avro;

import poc.kafka.AvroDynamicProducer;
import poc.kafka.AvroProducer;
import poc.kafka.AvroConsumer;

import java.util.Properties;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        // Press Opt+Enter with your caret at the highlighted text to see how
        // IntelliJ IDEA suggests fixing it.

        Properties envProps = new Properties();

        envProps.put("KafkaIP", "18.191.161.199:9092");
        envProps.put("KafkaSchemaRegistry", "http://18.191.161.199:8081");
        envProps.put("KafkaTopic", "avro");

        System.out.println("Producing Avro Message");
        AvroProducer avroProducer = new AvroProducer();
        avroProducer.writeToTopic(envProps);
        System.out.println("Message Produced");

        System.out.println("Producing Avro Message with Dynamic Schema");
        AvroDynamicProducer avroDynamicProducer = new AvroDynamicProducer();
        avroDynamicProducer.writeToTopic(envProps);
        System.out.println("Message Produced");

        System.out.println("Consuming Avro Messages with GenericRecord");
        AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.readFromTopic(envProps, "poc-consumer-group");
        System.out.println("Messages Consumed");

    }
}