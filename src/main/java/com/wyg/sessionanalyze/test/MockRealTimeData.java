package com.wyg.sessionanalyze.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	
	private KafkaProducer<Integer, String> producer;
	
	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[]{"Nanjing", "Suzhou"});
		provinceCityMap.put("Hubei", new String[]{"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[]{"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[]{"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[]{"Shijiazhuang", "Zhangjiakou"});
		createProducerConfig();
	}

	/*
	private ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.88.81:9092,192.168.88.82:9092,192.168.88.83:9092");
		return new ProducerConfig(props);
	}
	*/
	private KafkaProducer createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.1.130:9092,192.168.1.120:9092,192.168.1.110:9092");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.130:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producer =  new KafkaProducer(props);
		return producer;
	}
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			// 数据格式为：timestamp province city userId adId
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);  
			//producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));
			producer.send(new ProducerRecord<Integer, String>("AdRealTimeLog",log));
			//producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData MessageProducer = new MockRealTimeData();
		MessageProducer.start();
	}
	
}
