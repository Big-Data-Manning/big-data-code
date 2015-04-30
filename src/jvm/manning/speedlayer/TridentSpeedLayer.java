package manning.speedlayer;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import manning.schema.PersonID;
import manning.speedlayer.UniquesOverTime.PageviewScheme;
import static manning.speedlayer.CassandraState.*;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TridentSpeedLayer {


    public static class NormalizeURL extends BaseFunction {
        public void execute(TridentTuple tuple,
                            TridentCollector collector) {
            try {
                String urlStr = tuple.getString(0);
                URL url = new URL(urlStr);
                collector.emit(new Values(
                        url.getProtocol() +
                        "://" +
                        url.getHost() +
                        url.getPath()));
            } catch(MalformedURLException e) {
            }
        }
    }

    public static class ToHourBucket extends BaseFunction {
        private static final int HOUR_SECS = 60 * 60;

        public void execute(TridentTuple tuple,
                            TridentCollector collector) {
            int secs = tuple.getInteger(0);
            int hourBucket = secs / HOUR_SECS;
            collector.emit(new Values(hourBucket));
        }
    }

    public static TridentTopology pageviewsOverTime() {
        TridentTopology topology = new TridentTopology();
        TridentKafkaConfig kafkaConfig =
                new TridentKafkaConfig(
                        new ZkHosts(
                                "127.0.0.1:2181"),
                        "pageviews"
                        );
        kafkaConfig.scheme = new PageviewScheme();

        CassandraState.Options opts =
                new CassandraState.Options();
        opts.keySerializer = StringSerializer.get();
        opts.colSerializer = IntegerSerializer.get();

        StateFactory state =
                CassandraState.transactional(
                        "127.0.0.1",
                        "superwebanalytics",
                        "pageviewsOverTime",
                        opts);

        Stream stream =
            topology.newStream(
                    "pageviewsOverTime",
                    new TransactionalTridentKafkaSpout(
                            kafkaConfig))
                    .each(new Fields("url"),
                            new NormalizeURL(),
                            new Fields("normurl"))
                    .each(new Fields("timestamp"),
                            new ToHourBucket(),
                            new Fields("bucket"))
                    .project(new Fields("normurl", "bucket"));
        stream.groupBy(new Fields("normurl", "bucket"))
              .persistentAggregate(
                      state,
                 new Count(),
                 new Fields("count"));

        return topology;
    }

    public static class ExtractDomain extends BaseFunction {
        public void execute(TridentTuple tuple,
                            TridentCollector collector) {
            try {
                URL url = new URL(tuple.getString(0));
                collector.emit(new Values(url.getAuthority()));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class Visit extends ArrayList {
        public Visit(String domain, PersonID user) {
            super();
            add(domain);
            add(user);
        }
    }

    static class VisitInfo {
        public int startTimestamp;
        public Integer lastVisitTimestamp;

        public VisitInfo(int startTimestamp) {
            this.startTimestamp = startTimestamp;
            this.lastVisitTimestamp = startTimestamp;
        }

        public VisitInfo clone() {
            VisitInfo ret = new VisitInfo(this.startTimestamp);
            ret.lastVisitTimestamp = this.lastVisitTimestamp;
            return ret;
        }
    }

    public static class AnalyzeVisits
        extends BaseStateUpdater<MemoryMapState> {

        static final String LAST_SWEEP_TIMESTAMP = "lastSweepTs";
        static final int THIRTY_MINUTES_SECS = 30 * 60;

        public void updateState(
                MemoryMapState state,
                List<TridentTuple> tuples,
                TridentCollector collector) {
            for(TridentTuple t: tuples) {
                final String domain = t.getString(0);
                final PersonID user = (PersonID) t.get(1);
                final int timestampSecs = t.getInteger(2);
                Visit v = new Visit(domain, user);
                update(state, v, new ValueUpdater<VisitInfo>() {
                    public VisitInfo update(VisitInfo v) {
                        if(v==null) {
                            return new VisitInfo(timestampSecs);
                        } else {
                            VisitInfo ret = new VisitInfo(
                                                    v.startTimestamp);
                            ret.lastVisitTimestamp = timestampSecs;
                            return ret;
                        }
                    }
                });

                Integer lastSweep =
                    (Integer) get(state, LAST_SWEEP_TIMESTAMP);
                if(lastSweep==null) lastSweep = 0;

                List<Visit> expired = new ArrayList();
                if(timestampSecs > lastSweep + 60) {
                    Iterator<List<Object>> it = state.getTuples();
                    while(it.hasNext()) {
                        List<Object> tuple = it.next();
                        if(!LAST_SWEEP_TIMESTAMP.equals(tuple.get(0))) {
                            Visit visit = (Visit) tuple.get(0);
                            VisitInfo info = (VisitInfo) tuple.get(1);
                            if(info!=null) {
                                if(timestampSecs >
                                        info.lastVisitTimestamp + THIRTY_MINUTES_SECS) {
                                    expired.add(visit);
                                    if(info.startTimestamp ==
                                       info.lastVisitTimestamp) {
                                        collector.emit(new Values(domain, true));
                                    } else {
                                        collector.emit(new Values(domain, false));
                                    }
                                }
                            }
                        }
                    }
                    put(state, LAST_SWEEP_TIMESTAMP, timestampSecs);
                }

                for(Visit visit: expired) {
                    remove(state, visit);
                }
            }
        }
    }

    private static Object update(MapState s,
                                 Object key,
                                 ValueUpdater updater) {
        List keys = new ArrayList();
        List updaters = new ArrayList();
        keys.add(new Values(key));
        updaters.add(updater);
        return s.multiUpdate(keys, updaters).get(0);
    }

    private static Object get(MapState s, Object key) {
        List keys = new ArrayList();
        keys.add(new Values(key));
        return s.multiGet(keys).get(0);
    }

    private static void put(MapState s, Object key, Object val) {
        List keys = new ArrayList();
        keys.add(new Values(key));
        List vals = new ArrayList();
        vals.add(val);
        s.multiPut(keys, vals);
    }

    private static void remove(MemoryMapState s, Object key) {
        List keys = new ArrayList();
        keys.add(new Values(key));
        s.multiRemove(keys);
    }

    public static class BooleanToInt extends BaseFunction {
        public void execute(TridentTuple tuple,
                            TridentCollector collector) {
            boolean val = tuple.getBoolean(0);
            if(val) {
                collector.emit(new Values(1));
            } else {
                collector.emit(new Values(0));
            }
        }
    }

    public static class CombinedCombinerAggregator
        implements CombinerAggregator {

        CombinerAggregator[] _aggs;

        public CombinedCombinerAggregator(
                CombinerAggregator... aggs) {
            _aggs = aggs;
        }

        public Object init(TridentTuple tuple) {
            List<Object> ret = new ArrayList();
            for(CombinerAggregator agg: _aggs) {
                ret.add(agg.init(tuple));
            }
            return ret;
        }

        public Object combine(Object o1, Object o2) {
            List l1 = (List) o1;
            List l2 = (List) o2;
            List<Object> ret = new ArrayList();
            for(int i=0; i<_aggs.length; i++) {
                ret.add(
                   _aggs[i].combine(
                           l1.get(i),
                           l2.get(i)));
            }
            return ret;
        }

        public Object zero() {
            List<Object> ret = new ArrayList();
            for(CombinerAggregator agg: _aggs) {
                ret.add(agg.zero());
            }
            return ret;
        }
    }


    public static TridentTopology bounceRateOverTime() {
        TridentTopology topology = new TridentTopology();
        TridentKafkaConfig kafkaConfig =
                new TridentKafkaConfig(
                        new ZkHosts(
                                "127.0.0.1:2181"),
                        "pageviews"
                );
        kafkaConfig.scheme = new PageviewScheme();

        CassandraState.Options opts = new CassandraState.Options();
        opts.globalCol = "BOUNCE-RATE";
        opts.keySerializer = StringSerializer.get();
        opts.colSerializer = StringSerializer.get();

        topology.newStream(
                "bounceRate",
                new TransactionalTridentKafkaSpout(kafkaConfig))
                .each(new Fields("url"),
                      new NormalizeURL(),
                      new Fields("normurl"))
                .each(new Fields("normurl"),
                      new ExtractDomain(),
                      new Fields("domain"))
                .partitionBy(new Fields("domain", "user"))
                .partitionPersist(
                        new MemoryMapState.Factory(),
                        new Fields("domain", "user", "timestamp"),
                        new AnalyzeVisits(),
                        new Fields("domain", "isBounce"))
                .newValuesStream()
                .each(new Fields("isBounce"),
                      new BooleanToInt(),
                        new Fields("bint"))
                .groupBy(new Fields("domain"))
                .persistentAggregate(
                        CassandraState.transactional(
                                "127.0.0.1",
                                "superwebanalytics",
                                "bounceRate",
                                opts),
                        new Fields("bint"),
                        new CombinedCombinerAggregator(
                                new Count(),
                                new Sum()),
                        new Fields("count-sum"));
        return topology;
    }

    public static LocalCluster runPageviews() {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();

        cluster.submitTopology("pageviews", conf, pageviewsOverTime().build());
        return cluster;
    }

    public static LocalCluster runBounces() {
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();

        cluster.submitTopology("bounces", conf, bounceRateOverTime().build());
        return cluster;
    }
}
