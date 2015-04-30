package manning.speedlayer;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.spout.MultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import manning.schema.Data;
import manning.schema.PageViewEdge;
import manning.schema.PersonID;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.*;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import storm.kafka.KafkaConfig;
import storm.kafka.ZkHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static manning.test.Data.makeEquiv;
import static manning.test.Data.makePageview;

public class UniquesOverTime {

    public static void initTestData() throws Exception {
        List<Data> data = new ArrayList();
        data.add(makePageview(1, "http://foo.com/post1", 60));
        data.add(makePageview(2, "http://foo.com/post1", 60));
        data.add(makePageview(3, "http://foo.com/post1", 62));
        data.add(makePageview(2, "http://foo.com/post3", 62));
        data.add(makePageview(1, "http://foo.com/post1", 4000));
        data.add(makePageview(1, "http://foo.com/post2", 4000));
        data.add(makePageview(1, "http://foo.com/post2", 10000));
        data.add(makePageview(5, "http://foo.com/post3", 10600));

        Properties props = new Properties();

        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");

        ProducerConfig config = new ProducerConfig(props);

        TSerializer ser = new TSerializer();

        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        for(Data d: data) {
            KeyedMessage<String, byte[]> m = new KeyedMessage<String, byte[]>("pageviews", null, ser.serialize(d));
            producer.send(m);
        }

        producer.close();
    }

    public static class PageviewScheme implements MultiScheme {
        TDeserializer _des;

        @Override
        public Iterable<List<Object>> deserialize(byte[] bytes) {
            Data data = new Data();
            if(_des==null) _des = new TDeserializer();
            try {
                _des.deserialize(data, bytes);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            PageViewEdge pageview = data.get_dataunit().get_page_view();
            String url = pageview.get_page().get_url();
            PersonID user = pageview.get_person();
            List ret = new ArrayList();
            ret.add(new Values(user,
                               url,
                               data.get_pedigree()
                                   .get_true_as_of_secs()));
            return ret;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("user", "url", "timestamp");
        }
    }

    public static class ExtractFilterBolt extends BaseBasicBolt {
        private static final int HOUR_SECS = 60 * 60;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            PersonID user = (PersonID) tuple.getValue(0);
            String url = tuple.getString(1);
            int timestamp = tuple.getInteger(2);

            try {
                String domain = new URL(url).getAuthority();
                collector.emit(new Values(
                                domain,
                                url,
                                timestamp / HOUR_SECS,
                                user));
            } catch(MalformedURLException e) {
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("domain", "url", "bucket", "user"));
        }
    }

    /**
     * This is a simple illustrative example, though in reality you'll achieve much
     * greater throughput by batching the bolt as discussed in the book.
     */
    public static class UpdateCassandraBolt extends BaseBasicBolt {

        ColumnFamilyTemplate<String, Integer> _template;

        @Override
        public void prepare(
                Map conf,
                TopologyContext context) {
            Cluster cluster = HFactory.getOrCreateCluster(
                    "mycluster", "127.0.0.1");

            Keyspace keyspace = HFactory.createKeyspace(
                    "superwebanalytics", cluster);


            _template =
                    new ThriftColumnFamilyTemplate<String, Integer>(
                            keyspace,
                            "uniques",
                            StringSerializer.get(),
                            IntegerSerializer.get());
        }


        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String url = tuple.getString(1);
            int bucket = tuple.getInteger(2);
            PersonID user = (PersonID) tuple.getValue(3);

            HColumn<Integer, byte[]> hcol =
                    _template.querySingleColumn(
                            url,
                            bucket,
                            BytesArraySerializer.get());
            HyperLogLog hll;
            try {
                if(hcol==null) hll = new HyperLogLog(14);
                else hll = HyperLogLog.Builder.build(hcol.getValue());
                hll.offer(user);
                ColumnFamilyUpdater<String, Integer> updater =
                        _template.createUpdater(url);
                updater.setByteArray(bucket, hll.getBytes());
                _template.update(updater);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static LocalCluster run() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts("127.0.0.1:2181"),
                "pageviews",
                "/kafkastorm",
                "uniquesSpeedLayer"
        );
        spoutConfig.scheme = new PageviewScheme();

        builder.setSpout("pageviews",
                new KafkaSpout(spoutConfig), 2);

        builder.setBolt("extract-filter",
                new ExtractFilterBolt(), 4)
                .shuffleGrouping("pageviews");
        builder.setBolt("cassandra",
                new UpdateCassandraBolt(), 4)
                .fieldsGrouping("extract-filter",
                        new Fields("domain"));

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("uniques", conf, builder.createTopology());

        return cluster;
    }
}
