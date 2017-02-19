package com.agritsik.storm.sample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by andrey on 2/19/17.
 */
public class OrderTopology {

    /**
     * Consumes events
     */
    public static class TCOrderSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private int i = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(500);
            List<String> list = Arrays.asList("Italy", "Spain", "France", "Greece");
            Random random = new Random();

            String body = "{\"order_id\": \"WR%d\", \"country\":\"" + list.get(random.nextInt(list.size())) + "\"}";
            collector.emit(new Values(String.format(body, i++)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("json-order"));
        }

    }

    /**
     * Parses a json object
     */
    public static class TCOrderParserBold extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode node = objectMapper.readTree(input.getStringByField("json-order"));
                collector.emit(new Values(node.get("country").asText()));
                collector.ack(input);
            } catch (IOException e) {
                e.printStackTrace();
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("order-country"));
        }
    }


    /**
     * Saves to redis
     */
    public static class TCOrderSaverBolt extends AbstractRedisBolt {

        public TCOrderSaverBolt(JedisPoolConfig config) {
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


    public static class TCOrderPublisherBolt extends BaseRichBolt {

        private JedisPoolConfig config;
        private Jedis resource;

        public TCOrderPublisherBolt(JedisPoolConfig config) {
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
        builder.setSpout("order", new TCOrderSpout(), 2);
        builder.setBolt("order-parser", new TCOrderParserBold(), 2).shuffleGrouping("order");
        builder.setBolt("order-saver", new TCOrderSaverBolt(poolConfig), 2).shuffleGrouping("order-parser");
        builder.setBolt("order-pub", new TCOrderPublisherBolt(poolConfig), 2).shuffleGrouping("order-parser");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wrTopology", config, builder.createTopology());
        Utils.sleep(20000);
        cluster.killTopology("wrTopology");
        cluster.shutdown();
    }

}
