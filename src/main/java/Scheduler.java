import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.log4j.Logger;
import org.rocksdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Scheduler implements Runnable{


    @Override
    public void run() {

        try {
            Thread.sleep(40000);
            this.getData("/home/vagrant/rocksdb");

        }
        catch (Exception exception){

        }
    }


    private String getPath(String path) throws Exception{

        ProcessBuilder processBuilder1 = new ProcessBuilder("bash","-c","find "+ path + " -name job_* | head -1");
        Process process1 = processBuilder1.start();

        StringBuilder sb = new StringBuilder();
        InputStream in = process1.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }

        System.out.println("job : " + sb.toString());
        sb.append("/db");
        String finalPath = sb.toString();
        System.out.println("Final Path : " + finalPath);
        return finalPath;
    }

    private void getData(String path) throws Exception{


        File file = new File("/home/vagrant/output.txt");
        if(!file.exists()){
            file.createNewFile();
        }


        RocksDB.loadLibrary();
        String previousIntColumnFamily = "my-state";
        byte[] previousIntColumnFamilyBA = previousIntColumnFamily.getBytes(StandardCharsets.UTF_8);



        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(previousIntColumnFamilyBA, cfOpts)
            );

            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            String dbPath = getPath(path);
            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandleList)) {


                TypeInformation<Tuple2<Integer,Integer>> resultType = TypeInformation.of(new TypeHint<Tuple2<Integer,Integer>>() {
                });
                TypeSerializer<Tuple2<Integer,Integer>> serializer = resultType.createSerializer(new ExecutionConfig());


                try {
                    for(ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList){
                        byte[] name = columnFamilyHandle.getName();
                        System.out.println(new String(name));

                        RocksIterator iterator =  db.newIterator(columnFamilyHandle);
                        iterator.seekToFirst();
                        iterator.status();

                        while (iterator.isValid()) {
                            byte[] key = iterator.key();

                            Tuple2<Integer,Integer> tuple2 = serializer.deserialize(new DataInputDeserializer(iterator.value()));

                            System.out.println(tuple2.f0 + " - "  +tuple2.f1);

                            org.apache.log4j.Logger logger = Logger.getLogger(KafkaFlink.class);
                            logger.info("------Data------");
                            logger.info("key : " + tuple2.f0 + " | Value : " + tuple2.f1);

                            FileOutputStream fos = new FileOutputStream(file);
                            String data = "Key : " + tuple2.f0 + " | Value : " + tuple2.f1;
                            fos.write(data.getBytes(StandardCharsets.UTF_8));
                            fos.close();
                            iterator.next();

                        }
                    }
                }finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle :
                            columnFamilyHandleList) {
                        columnFamilyHandle.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
