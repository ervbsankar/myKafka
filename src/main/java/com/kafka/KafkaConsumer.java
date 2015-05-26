package com.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaConsumer extends Thread{
	
	private final static String TOPIC = "first";
	private final ConsumerConnector consumerConnector;
	

	public static void main(String[] args) {
		System.out.println("entered to Main");
		KafkaConsumer kafkaConsumer = new KafkaConsumer();
		kafkaConsumer.start();
	}

	public KafkaConsumer() {
		System.out.println("entered to Constructor");
		Properties props = new Properties();
		props.put("zookeeper.connect", "75.72.139.144:2181");
		props.put("group.id", "group2");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
	    props.put("auto.offset.reset","largest");
		/* below command will timeout have to handle this exception ConsumerTimeoutException */
		/*props.put("consumer.timeout.ms","1000");*/
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	}
	
	
	  @Override
	    public void run() {
		    System.out.println("entered to run");
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(TOPIC, new Integer(2));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(1);
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while(it.hasNext())
	            System.out.println(new String(it.next().message()));
	 
	    }

}
