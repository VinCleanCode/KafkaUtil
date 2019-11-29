package com.rp.lib.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers","");
		props.put("group.id","");
		props.put("key.deserializer","");
		props.put("value.deserializer","");
		props.put("compression.type", "none");//default: none
		props.put("linger.ms", 0);//default: 0
		props.put("buffer.memory", 33554432);//default: 33554432 byte = 32 MB
		props.put("max.request.size",10485760);//set to 10MB, default: 1MB

		Producer<String, String> producer = new KafkaProducer<>(props);

		for(int i=0; i<100; i++){
			producer.send(new ProducerRecord<>("topic",Integer.toString(i),Integer.toString(i)));
		}
		producer.close();
	}
}
