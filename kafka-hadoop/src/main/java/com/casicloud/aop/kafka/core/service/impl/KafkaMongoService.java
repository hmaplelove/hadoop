package com.casicloud.aop.kafka.core.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.core.service.KafkaService;

public class KafkaMongoService implements KafkaService{
	private static final Logger logger = LoggerFactory.getLogger(KafkaMongoService.class);
	@Autowired
	private MongoTemplate mongoTemplate;
	@Override
	public void processMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		onMessage(message);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void onMessage(Map<Object, Map<Object, Object>> message) throws Exception {

		for (Map.Entry < Object,Map<Object, Object>>entry:  message.entrySet()){
            for (Entry<Object, Object> msg : entry.getValue().entrySet()) {
            	List<String> list=(List<String>) msg.getValue();
            	for (String json : list) {
            		HashMap params=JSON.parseObject(json, HashMap.class);
            		logger.info("msg=========>"+params);
            		String collectionName="iot";
            		if (!mongoTemplate.collectionExists(collectionName)) {
            			mongoTemplate.createCollection(collectionName);
					}
            		mongoTemplate.save(params, collectionName);
				}
            }
        }
	
	}

}
