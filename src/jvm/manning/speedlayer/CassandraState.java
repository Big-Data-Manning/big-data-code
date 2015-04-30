package manning.speedlayer;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import storm.trident.state.*;
import storm.trident.state.Serializer;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraState<T> implements IBackingMap<T> {
    public static interface CassandraSerializerFactory extends Serializable {
        public me.prettyprint.hector.api.Serializer getSerializer();
    }

    public static class StringSerializer implements CassandraSerializerFactory {
        public me.prettyprint.hector.api.Serializer getSerializer() {
            return me.prettyprint.cassandra.serializers.StringSerializer.get();
        }

        public static StringSerializer get() {
            return new StringSerializer();
        }
    }

    public static class IntegerSerializer implements CassandraSerializerFactory {
        public me.prettyprint.hector.api.Serializer getSerializer() {
            return me.prettyprint.cassandra.serializers.IntegerSerializer.get();
        }

        public static IntegerSerializer get() {
            return new IntegerSerializer();
        }
    }

    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
    }};

    public static class Options<T> implements Serializable {
        public int localCacheSize = 1000;
        public Object globalKey = "$GLOBAL$";
        public Object globalCol = "$GLOBAL$";
        public CassandraSerializerFactory keySerializer = StringSerializer.get();
        public CassandraSerializerFactory colSerializer = StringSerializer.get();
        public Serializer<T> valueSerializer = null;
    }


    public static StateFactory transactional(String connStr, String keyspace, String columnFamily) {
        return transactional(connStr, keyspace, columnFamily, new Options());
    }

    public static StateFactory transactional(String connStr, String keyspace, String columnFamily, Options<TransactionalValue> opts) {
        return new Factory(connStr, keyspace, columnFamily, StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional(String connStr, String keyspace, String columnFamily) {
        return nonTransactional(connStr, keyspace, columnFamily, new Options());
    }

    public static StateFactory nonTransactional(String connStr, String keyspace, String columnFamily, Options<Object> opts) {
        return new Factory(connStr, keyspace, columnFamily, StateType.NON_TRANSACTIONAL, opts);
    }

    protected static class Factory implements StateFactory {
        StateType _type;
        String _connStr;
        String _keyspace;
        String _columnFamily;
        Serializer _valueSer;
        Options _opts;

        public Factory(String connStr, String keyspace, String columnFamily, StateType type, Options options) {
            _type = type;
            _connStr = connStr;
            _keyspace = keyspace;
            _columnFamily = columnFamily;
            _opts = options;
            if(options.valueSerializer==null) {
                _valueSer = DEFAULT_SERIALZERS.get(type);
                if(_valueSer==null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                _valueSer = options.valueSerializer;
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
            Cluster cluster = HFactory.getOrCreateCluster(
                    "mycluster", _connStr);

            Keyspace keyspace = HFactory.createKeyspace(
                    _keyspace, cluster);
            ThriftColumnFamilyTemplate template =
                    new ThriftColumnFamilyTemplate(
                            keyspace,
                            _columnFamily,
                            _opts.keySerializer.getSerializer(),
                            _opts.colSerializer.getSerializer());

            CassandraState s = new CassandraState(template, _opts, _valueSer);

            CachedMap c = new CachedMap(s, _opts.localCacheSize);
            MapState ms;
            if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.build(c);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(ms, new Values(_opts.globalKey));
        }


    }

    private Options _opts;
    private Serializer<T> _valueSer;
    private ThriftColumnFamilyTemplate _template;



    public CassandraState(ThriftColumnFamilyTemplate template, Options opts, Serializer<T> valueSer) {
        _opts = opts;
        _valueSer = valueSer;
        _template = template;


    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> ret = new ArrayList();
        for(List<Object> keyCol: keys) {
            checkKeyCol(keyCol);
            HColumn<Object, byte[]> hcol = _template.querySingleColumn(keyCol.get(0), getCol(keyCol), BytesArraySerializer.get());
            T val = null;
            if(hcol!=null) {
                byte[] ser = hcol.getValue();
                val = _valueSer.deserialize(ser);
            }
            ret.add(val);
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        for(int i=0; i<keys.size(); i++) {
            List<Object> keyCol = keys.get(i);
            checkKeyCol(keyCol);
            T val = vals.get(i);
            byte[] ser = _valueSer.serialize(val);
            ColumnFamilyUpdater updater =
                    _template.createUpdater(keyCol.get(0));
            updater.setByteArray(getCol(keyCol), ser);
            _template.update(updater);
        }
    }

    private Object getCol(List<Object> keyCol) {
        Object col;
        if(keyCol.size()==1) {
            col = _opts.globalCol;
        } else {
            col = keyCol.get(1);
        }
        return col;
    }

    private static void checkKeyCol(List<Object> keyCol) {
        if(keyCol.size()!=1 && keyCol.size()!=2) {
            throw new RuntimeException("Trident key should be a 2-tuple of key/column or a 1-tuple of key. Invalid: " + keyCol.toString());
        }
    }
}
