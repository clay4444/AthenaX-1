package com.mydashboard.athena.catalog.impl;

import java.io.Serializable;

/*import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.TOPIC_NAME_KEY;*/

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*import com.uber.athenax.tests.ITestUtil.KafkaCatalog;
import com.uber.athenax.tests.ITestUtil.KafkaInputExternalCatalogTable;*/
import com.uber.athenax.vm.api.AthenaXTableCatalog;
import com.uber.athenax.vm.api.AthenaXTableCatalogProvider;

public class MydashboardAthenaCatalogProvider implements AthenaXTableCatalogProvider {
	
	private static final Logger LOG = LoggerFactory.getLogger(MydashboardAthenaCatalogProvider.class);

	static final String DEST_TOPIC = "bar";
	static final String SOURCE_TOPIC = "foo";

	private static final long STABILIZE_SLEEP_DELAYS = 3000;
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static String brokerAddress = "127.0.0.1:9092";
	
	/**
	   * The prefix of all Kafka configurations that will be passed into
	   * the Kafka consumer / producer.
	   */
	  public static final String KAFKA_CONFIG_PREFIX = "kafka.";
	
	/**
	   * The name of the Kafka topic to be read or written.
	   */
	  public static final String TOPIC_NAME_KEY = "athenax.kafka.topic.name";
	  
	  
	  static class KafkaInputExternalCatalogTable extends ExternalCatalogTable implements Serializable {
		    private static final TableSchema SCHEMA = new TableSchema(
		        new String[] {"id"},
		        new TypeInformation<?>[] {BasicTypeInfo.INT_TYPE_INFO});

		    KafkaInputExternalCatalogTable(Map<String, String> properties) {
		      super("kafka+json", SCHEMA, properties, null, null, null, null);
		    }
		  }
	
	
	public static class KafkaCatalog implements AthenaXTableCatalog {
	    private static final long serialVersionUID = -1L;

	    private final String broker;
	    private final List<String> availableTables;

	    KafkaCatalog(String broker, List<String> availableTables) {
	      this.broker = broker;
	      this.availableTables = availableTables;
	    }

	    @Override
	    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
	      Map<String, String> sourceTableProp = new HashMap<>();
	      sourceTableProp.put(KAFKA_CONFIG_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, tableName);
	      sourceTableProp.put(KAFKA_CONFIG_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
	      sourceTableProp.put(KAFKA_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	      sourceTableProp.put(TOPIC_NAME_KEY, tableName);

	      System.out.println("#$#$#$#$#$# inside the KafkaCatalog class ExternalCatalogTable method... @#@#@#@#@");
	      return new KafkaInputExternalCatalogTable(sourceTableProp);
	    	
	    //return null;
	    }

	    @Override
	    public List<String> listTables() {
	      return availableTables;
	    }

	    @Override
	    public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
	      throw new CatalogNotExistException(dbName);
	    }

	    @Override
	    public List<String> listSubCatalogs() {
	      return Collections.emptyList();
	    }
	  }

	@Override
	public Map<String, AthenaXTableCatalog> getInputCatalog(String cluster) {
		Preconditions.checkNotNull(brokerAddress);
		LOG.info("Inside the getInputCatalog on MydashboardAthenaCatalogProvider class | parameter : {} ",cluster);
		return Collections.singletonMap("input",
				new KafkaCatalog(brokerAddress, Collections.singletonList(SOURCE_TOPIC)));
	}

	@Override
	public AthenaXTableCatalog getOutputCatalog(String cluster, List<String> outputs) {
		Preconditions.checkNotNull(brokerAddress);
		LOG.info("Inside the getOutputCatalog on MydashboardAthenaCatalogProvider class | parameter cluster : {} | parameter outputs : {}",cluster,outputs);
		return new KafkaCatalog(brokerAddress, Collections.singletonList(DEST_TOPIC));
	}

	/*private static KafkaProducer<byte[], byte[]> getProducer(String brokerList) {
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		return new KafkaProducer<>(prop);
	}*/

	static KafkaConsumer<byte[], byte[]> getConsumer(String groupName, String brokerList) {
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

	static String brokerAddress() {
		brokerAddress = "127.0.0.1:9092";
		return brokerAddress;
	}

}
