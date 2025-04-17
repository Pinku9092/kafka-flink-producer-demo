package com.pinku;

import com.pinku.pojos.Employee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaFlinkProducerDemoApplication  {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment  env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");

		FlinkKafkaProducer<Employee> kafkaProducer = new FlinkKafkaProducer<Employee>(
				"testtopic1",
				new EmployeeSerializationSchema("testtopic1"),
				kafkaProperties,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);

		
		env.addSource(new MongoSource())
				.name("MongoDB Source")
				.addSink(kafkaProducer)
				.name("Kafka Sink");
		 env.execute("Mongo to Kafka Flink Producer job");
	}

}
