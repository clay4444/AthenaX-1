package com.mydashboard.athena.catalog.impl;

import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.DEST_TOPIC;
import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.brokerAddress;
import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.getConsumer;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.fasterxml.jackson.databind.ObjectMapper;

public class Consumer1 {

	private static final Logger LOG = LoggerFactory.getLogger(Consumer1.class);
	//private static final ObjectMapper MAPPER = new ObjectMapper();

	/*public void Consumer() {
		try (KafkaConsumer<byte[], byte[]> consumer = getConsumer("observer", brokerAddress())) {
			consumer.subscribe(Collections.singletonList(DEST_TOPIC));
			boolean found = false;
			while (!found) {
				ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
				for (ConsumerRecord<byte[], byte[]> r : records.records(DEST_TOPIC)) {
					@SuppressWarnings("unchecked")
					Map<String, Object> m = MAPPER.readValue(r.value(), Map.class);
					if ((Integer) m.get("id") == 2) {
						found = true;
					}
				}
			}
			
			 * ServerContext.INSTANCE.executor().shutdown();
			 * ServerContext.INSTANCE.instanceManager().close();
			 
		}catch (Exception ex){
			ex.printStackTrace();
		}
	}*/

}
