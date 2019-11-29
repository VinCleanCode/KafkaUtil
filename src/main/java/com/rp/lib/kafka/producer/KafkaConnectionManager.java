/** 
 * 版本信息： 
 * 创建时间：Jan 28, 2015-1:19:31 PM 
 * **********Copyright (c) 2015  新蛋科技(成都)公司-版权所有********** 
 */
package com.rp.lib.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** 
 * 类 描 述 ： 
 * 创 建 人 ： Lam
 * 修 改 人 ： Chuck
 * 修改备注 ：
 * 创建时间 ： Jan 28, 2015 1:19:31 PM
 */
public class KafkaConnectionManager {
	private static final Logger log = LogManager.getLogger();

	private static KafkaConnectionManager kafkaConnectionManager = null;
	private static final Object lock = new Object();
	private static List<Producer<String, String>> list_producer = new ArrayList<Producer<String,String>>();
	private static Properties prConfig = null;
	private static int num_prducer = 0;//获取producer 计数
	/**
	 * 创建一个新的实例 KafkaConnectionManager
	 */
	private KafkaConnectionManager(){
		if(list_producer.isEmpty()){
			initProdecerPool();
		}
	}
	/**
	 * 方法描述 ： 加载配置文件建立链接
	 * @return  
	 * 创 建 人 ： Lam
	 * 修 改 人 ： Vincent
	 * 修改备注 ： Modify to get config from loadKafkaConfig
	 * 返回类型 ： ProducerConfig
	 * 创建时间 ： Jan 28, 2015 1:42:09 PM
	 */
	private Properties getProducerConfig(){
		Properties props = new Properties();
		try {
			//acks all:所有消息同步到 slave 节点后才会返回成功的确认消息给客户端。
			props.put("compression.type", "none");//default: none
			props.put("linger.ms", 0);//default: 0
			props.put("buffer.memory", 33554432);//default: 33554432 byte = 32 MB
			props.put("max.request.size",10485760);//set to 10MB, default: 1MB
		} catch (Exception e) {
			log.error("KafkaConnectionManager getProducerConfig error:{},\n{}",e.getMessage(),e.getStackTrace().toString());
			e.printStackTrace();
		}
		return props;
	}
	/**
	 * 方法描述 ： 初始化 producer pool
	 * 创 建 人 ： Lam
	 * 修 改 人 ： Chuck 12/2/2017
	 * 修改备注 ： The producer is thread safe and sharing a single producer instance across threads will generally be faster than having multiple instances.
	 * 返回类型 ： void
	 * 创建时间 ： Jan 30, 2015 10:39:48 AM
	 */
	private void initProdecerPool(){
		if(prConfig == null){
			prConfig = getProducerConfig();
		}
		int connectionQuantityPerBroker = 1;
		for(int i=0; i<connectionQuantityPerBroker; i++){
			Producer<String, String> producer = new KafkaProducer<>(prConfig);
			if(producer!=null){
				list_producer.add(producer);
			}
		}
	}
		
	/**
	 * 方法描述 ： 获取一个producer.
	 * @return producer object.
	 * 创 建 人 ： Lam
	 * 修 改 人 ： Chuck
	 * 修改备注 ： Producer 是线程安全的，且可以往任何 topic 发送消息。一般一个应用，对应一个 producer 就足够了。
	 * 返回类型 ： Producer<String,String>
	 * 创建时间 ： Jan 28, 2015 2:13:45 PM
	 * 修改时间 ： 12/2/2017
	 */
	public Producer<String, String> getProducer(){
		Producer<String, String> producer = null;
		try {
			if(list_producer.size()==0){
				log.info("KafkaConnectionManager the list producer size: {}",list_producer);
				return producer;
			}
		     num_prducer += 1;
		     num_prducer %= list_producer.size();
		     producer = list_producer.get(num_prducer);
		} catch (Exception e) {
			log.error("KafkaConnectionManager Producer error:{},\n{}",e.getMessage(),e.getStackTrace().toString());
			e.printStackTrace();
		}
		return producer;
	}
	/**
	 * 方法描述 ： 
	 * @return  kafkaConnectionManager instance
	 * 创 建 人 ：Lam 
	 * 修 改 人 ： 
	 * 修改备注 ： 
	 * 返回类型 ：KafkaConnectionManager 
	 * 创建时间 ：Jan 28, 2015 2:13:50 PM
	 */
	public static KafkaConnectionManager getInstance(){
		if (kafkaConnectionManager == null) {
			synchronized (lock) {
				if (kafkaConnectionManager == null) {
					kafkaConnectionManager = new KafkaConnectionManager();
				}
			}
		}
		return kafkaConnectionManager;
	}
}
