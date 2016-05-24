package kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		List<Future<RecordMetadata>> results = new ArrayList<>();
		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			for (int i = 0; i < 100; i++) {
				Future<RecordMetadata> result = producer
						.send(new ProducerRecord<String, String>("mytopic1", Integer.toString(i), Integer.toString(i)));
				results.add(result);
			}
		} 
		
		results.forEach(MyProducer::printResult);
	}
	
	private static void printResult(Future<RecordMetadata> result){
		try {
			RecordMetadata rm = result.get();
			System.out.println("topic:" + rm.topic() + ", partition:" + rm.partition() + ",offset:" + rm.offset());
		} catch (InterruptedException | ExecutionException e) {
			System.out.println("Exception happened:" + e.getMessage());
		}
	}

}
