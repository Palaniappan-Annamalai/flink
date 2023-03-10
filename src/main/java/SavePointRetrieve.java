import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SavePointRetrieve {


    public DataStream<String> read(StreamExecutionEnvironment env) throws Exception{


      SavepointReader savepointReader = SavepointReader.read(env, "file:/D:/Flink/flink-1.9.0/flink-1.9.0/savepoint/savepoint-fb4e9a-a827aa9ed102"
              , new MemoryStateBackend());


      DataStream<String> dataStream = savepointReader.readKeyedState("1",new ReaderFunction());



return dataStream;

    }


    public static class ReaderFunction extends KeyedStateReaderFunction<Tuple1<Integer>,String> {

        ValueState<Tuple2<Integer,Integer>> state;



        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Tuple2<Integer,Integer>> stateDescriptor = new ValueStateDescriptor<Tuple2<Integer,Integer>>("my-state", TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
            }));
            state = getRuntimeContext().getState(stateDescriptor);

        }

        @Override
        public void readKey(Tuple1<Integer> integerTuple1, Context context, Collector<String> collector) throws Exception {
            KeyedState keyedState = new KeyedState();
            keyedState.key = integerTuple1.f0;
            keyedState.value = state.value().f1;
            String val ="Key : " + keyedState.key.toString() + ", Value : " + keyedState.value.toString();
            collector.collect(val);
        }





    }



    }


class KeyedState{
    Integer key;

    Integer value;

}