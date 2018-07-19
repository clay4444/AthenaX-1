/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.WebServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/*import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.DEST_TOPIC;
import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.brokerAddress;
import static com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider.getConsumer;*/

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AthenaXServer {
  private static final Options CLI_OPTIONS = new Options()
      .addOption(null, "conf", true, "The configuration file");
  
  	static final String DEST_TOPIC = "test";
	static final String SOURCE_TOPIC = "foo";
	private static String brokerAddress;
	private static final ObjectMapper MAPPER = new ObjectMapper();

  private void start(AthenaXConfiguration conf) throws Exception {
    ServerContext.INSTANCE.initialize(conf);
    ServerContext.INSTANCE.start();
    try (WebServer server = new WebServer(URI.create(conf.masterUri()))) {
      server.start();
      
      try (KafkaConsumer<String, String> consumer = getConsumer("observer", brokerAddress())) {
			consumer.subscribe(Collections.singletonList(DEST_TOPIC));
			boolean found = false;
			while (!found) {
				//ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
				/*for (ConsumerRecord<byte[], byte[]> r : records.records(DEST_TOPIC)) {
					@SuppressWarnings("unchecked")
					Map<String, Object> m = MAPPER.readValue(r.value(), Map.class);
					if ((Integer) m.get("id") == 2) {
						found = true;
					}
				}*/
				
				ConsumerRecords<String, String> records = consumer.poll(1000);
		         for (ConsumerRecord<String, String> record : records)
		         
		         // print the offset,key and value for the consumer records.
		         System.out.printf("offset = %d, key = %s, value = %s\n", 
		            record.offset(), record.key(), record.value());
			}
			
			System.out.println("found : "+found);
			
			 //ServerContext.INSTANCE.executor().shutdown();
			 //ServerContext.INSTANCE.instanceManager().close();
			 
		}catch (Exception ex){
			ex.printStackTrace();
		}
      
      Thread.currentThread().join();
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(CLI_OPTIONS, args);
    if (!line.hasOption("conf")) {
      System.err.println("No configuration file is specified");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("athenax-server", CLI_OPTIONS);
      System.exit(1);
    }

    try {
      String confFile = line.getOptionValue("conf");
      AthenaXConfiguration conf = AthenaXConfiguration.load(Paths.get(confFile).toFile());
      new AthenaXServer().start(conf);
    } catch (IOException | ClassNotFoundException e) {
      System.err.println("Failed to parse configuration.");
      throw e;
    }
  }
  
  	static String brokerAddress() {
		brokerAddress = "127.0.0.1:9092";
		return brokerAddress;
	}
  	
  	static KafkaConsumer<String, String> getConsumer(String groupName, String brokerList) {
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class.getCanonicalName());
		return new KafkaConsumer<>(prop);
	}
}
