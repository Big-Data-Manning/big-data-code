package manning.tap;

import backtype.cascading.tap.PailTap;
import backtype.cascading.tap.PailTap.PailTapOptions;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;


public class DataPailTap extends PailTap {
    public static class DataPailTapOptions {
        public PailSpec spec = null;
        public String fieldName = "data";

        public DataPailTapOptions() {

        }

        public DataPailTapOptions(PailSpec spec, String fieldName) {
            this.spec = spec;
            this.fieldName = fieldName;
        }
    }

    public DataPailTap(String root, DataPailTapOptions options) {
        super(root, new PailTapOptions(PailTap.makeSpec(options.spec, getSpecificStructure()), options.fieldName, null, null));
    }

    public DataPailTap(String root) {
        this(root, new DataPailTapOptions());
    }

    protected static PailStructure getSpecificStructure() {
        return new DataPailStructure();
    }
}