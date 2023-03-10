import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount{

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost",9999)
                .map(new Convert())
                .map(new Mapping())
                .keyBy(0)
                .map(new MyMap())
                .print();

        env.execute();

    }

    private static class MyMap extends RichMapFunction<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>{

        private transient ValueState<Tuple2<Integer,Integer>> valueState;


        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Integer,Integer>> descriptor =
                    new ValueStateDescriptor<>("my-state", TypeInformation.of(new TypeHint<Tuple2<Integer,Integer>>() {
                    }));

            valueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            Tuple2<Integer,Integer> tuple2 = valueState.value();

            tuple2.f1 = valueState.value().f1 + value.f1;
            tuple2.f0 = value.f0;
            valueState.update(tuple2);
            return tuple2;
        }
    }

    private static class Mapping implements MapFunction<Integer, Tuple2<Integer,Integer>>{

        @Override
        public Tuple2<Integer, Integer> map(Integer value) throws Exception {
            return new Tuple2<>(value,1);
        }
    }
    private static class Convert implements MapFunction<String,Integer>{

        @Override
        public Integer map(String value) throws Exception {
            return Integer.parseInt(value);
        }
    }
}