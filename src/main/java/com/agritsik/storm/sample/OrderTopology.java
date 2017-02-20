package com.agritsik.storm.sample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by andrey on 2/19/17.
 */
public class OrderTopology {


    /**
     * Parses a json object
     */
    public static class JsonParserBold extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String jsonString = input.getStringByField("str");
                JsonNode node = objectMapper.readTree(jsonString);
                JsonNode country = node.get("country");

                if (country == null) {
                    System.out.println("There is no country. Skip the message: " + jsonString);
                } else {
                    collector.emit(new Values(country.asText()));
                }

                collector.ack(input);
            } catch (IOException e) {
                System.out.println("Error. Skip the message: " + e.getMessage());
                e.printStackTrace();
                collector.ack(input); // skip the message
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("order-country"));
        }
    }


    /**
     * Saves into Redis
     */
    public static class CountrySaverBolt extends AbstractRedisBolt {

        public CountrySaverBolt(JedisPoolConfig config) {
            super(config);
        }

        @Override
        public void execute(Tuple input) {
            JedisCommands jedis = getInstance();
            jedis.zincrby("top:o",
                    1,
                    input.getStringByField("order-country"));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    /**
     * Publishes into Redis
     */
    public static class CountryPublisherBolt extends BaseRichBolt {

        private JedisPoolConfig config;
        private Jedis resource;

        public CountryPublisherBolt(JedisPoolConfig config) {
            this.config = config;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            JedisPool jedisPool = new JedisPool(new redis.clients.jedis.JedisPoolConfig(), config.getHost(), config.getPort());
            resource = jedisPool.getResource();
        }

        @Override
        public void execute(Tuple input) {
            resource.publish("test", input.getStringByField("order-country"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("192.168.99.100").setPort(6379)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("order", new KafkaSpout(buildKafkaSpoutConfig()), 2);
        builder.setBolt("order-parser", new JsonParserBold(), 2).shuffleGrouping("order");
        builder.setBolt("order-saver", new CountrySaverBolt(poolConfig), 2).shuffleGrouping("order-parser");
        builder.setBolt("order-pub", new CountryPublisherBolt(poolConfig), 2).shuffleGrouping("order-parser");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wrTopology", config, builder.createTopology());
//        Utils.sleep(20000);
//        cluster.killTopology("wrTopology");
//        cluster.shutdown();
    }


    /**
     * Builds config for KafkaSpout
     * @return SpoutConfig
     */
    private static SpoutConfig buildKafkaSpoutConfig() {
        String zkConnString = "192.168.99.100:2181";
        String topic = "my-first-topic";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return kafkaSpoutConfig;
    }

}
