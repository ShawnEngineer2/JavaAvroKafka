package poc.kafka;

import java.util.Properties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;

import poc.avro.Config;

public class AvroConsumer {

    public void readFromTopic(Config envProps, String consumerGroupName) {

        final String AUTO_OFFSET_RESET = "earliest";
//Set properties for the consumer
        Properties consumerProps = new Properties();

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.get(Config.PropKeys.KAFKA_IP));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,envProps.get(Config.PropKeys.KAFKA_SCHEMA_REGISTRY));


        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(envProps.get(Config.PropKeys.KAKFA_TOPIC)));


        while(true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            int msgCount = 0;

            for(ConsumerRecord<String, GenericRecord> record : records) {
                System.out.println("Message Content: " + record.value().get("fname"));
                System.out.println("Message Content: " + record.value().get("lname"));
                System.out.println("Email Addresses: ");

                GenericData.Array<GenericRecord> emailAddresses = (GenericData.Array<GenericRecord>) record.value().get("emailAddresses");

                for(GenericRecord email : emailAddresses) {
                    System.out.println("Email: " + email.get("email"));
                }

                msgCount++;
            }

            //Commit read events
            consumer.commitAsync();

            if (msgCount > 0) {
                break;
            }

        }

    }

}
