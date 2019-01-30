import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args){
       // runProducer();
       runConsumer();
    }

    private static void runConsumer() {
        Consumer<Integer, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;

            }
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });

            consumer.commitAsync();
        }

        consumer.close();
    }

    private static void runProducer() {
        Producer<Integer,String> producer = ProducerCreator.createProducer();

        for(int i = 0; i< 1012;i++){
            final ProducerRecord<Integer,String> producerRecord = new ProducerRecord<Integer, String>(IKafkaConstants.TOPIC_NAME, i,
                    "this is record "+ i);
            try {
                RecordMetadata recordMetadata = producer.send(producerRecord).get();
                System.out.println("Record sent with key " + i + " to partition " + recordMetadata.partition()
                + " with offset " + recordMetadata.offset());
            }
            catch (ExecutionException | InterruptedException e){
                System.out.println(e);
            }

        }
    }
}
