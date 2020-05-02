package com.cosh.transformer.sequencefile;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

public class SequenceFileToJsonTransformerImpl implements SequenceFileTransformer {

    private String _sourceFile;
    private String _destinationPath;
    private long _batchSize;

    public SequenceFileToJsonTransformerImpl(String sourceFile, String destinationPath, long batchSize)
    {
        this._batchSize=batchSize;
        this._destinationPath=destinationPath;
        this._sourceFile=sourceFile;
    }

    @Override
    public void transformSequenceFile() {
        Configuration conf = HBaseConfiguration.create();
        HBaseConfiguration.create();

        conf.set("io.file.buffer.size", "100000");
        conf.setStrings("io.serializations", conf.get("io.serializations"), MutationSerialization.class.getName(),ResultSerialization.class.getName());

        SequenceFile.Reader reader = null ;

        try {

            final Path seqfile = new Path( _sourceFile ) ;
            FileSystem rawFs = new LocalFileSystem();
            rawFs.setConf(conf);

            SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(seqfile);
            reader = new SequenceFile.Reader(conf, fileOption);

            WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();

            Result result = null;

            long count = 0;

            StringBuffer sb = new StringBuffer();

            while (reader.next(key)){
                count++;

                HashMap<String, String> value_map = new HashMap<>();
                String skey = Bytes.toString(((ImmutableBytesWritable)key).get());
                result = (Result) reader.getCurrentValue(result);
                NavigableMap<byte[], byte[]> resultMap = result.getFamilyMap(Bytes.toBytes("d"));

                resultMap.forEach((k, v) -> {
                    value_map.put(Bytes.toString(k), Bytes.toString(v));
                });

                Map<String, Object> output_map = new HashMap<String, Object>();
                output_map.put("hBaseKey", skey);
                output_map.put("hBaseValue", value_map);

                Gson gson = new Gson();
                sb.append(gson.toJson(output_map));

                if(count % _batchSize == 0)
                {
                    persist(count, sb);
                    sb=new StringBuffer();
                }

            }

            reader.close();

            persist(count, sb);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void persist(long count, StringBuffer sb) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(_destinationPath + count + ".json"));
        writer.write(sb.toString());
        writer.close();
    }
}
