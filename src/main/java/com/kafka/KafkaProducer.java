package com.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer extends Thread{
 
	public static void main(String[] args) {
		KafkaProducer kafkaProducer = new KafkaProducer();
		kafkaProducer.start();
	}
	  private final kafka.javaapi.producer.Producer<Integer, String> producer;
	  private final String topic="first";
	  private final Properties props = new Properties();
	  public KafkaProducer()
	  {
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
	    props.put("metadata.broker.list", "75.72.139.144:9092");
	    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
	    }
	  
	  @Override
	  public void run() {
		  System.out.println("run");
	    int messageNo = 1;
	    while(true)
	    {
	      String messageStr = new String("Message_" + messageNo);
	      producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
	      messageNo++;
	    }
	  }
}
