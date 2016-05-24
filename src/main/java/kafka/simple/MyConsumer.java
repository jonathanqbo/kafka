package kafka.simple;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Consumers in same group will only consume the same message once
 */
public class MyConsumer {
	
	private String id;
	
	public MyConsumer(String id) {
		this.id = id;
	}

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		// consumers in same group will only consume the same message once
		props.put("group.id", "mygroup_" + id);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)){
			consumer.subscribe(Arrays.asList("mytopic1"));
			
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records)
					System.out.printf("consumer = %s, offset = %d, key = %s, value = %s\n", id, record.offset(), record.key(), record.value());
			}
		}
	}
	
}
