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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;

class HBaseOutput {
    public String hbaseKey;
    public HashMap<String, String> hbaseValue = new HashMap<>();

    HBaseOutput(String hbaseKey) {
        this.hbaseKey = hbaseKey;
    }
}

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

            Gson gson = new Gson();

            Result result = null;

            long count = 0;

            ArrayList<HBaseOutput> outputList = new ArrayList<HBaseOutput>();

            while (reader.next(key)){
                count++;

                String skey = Bytes.toString(((ImmutableBytesWritable)key).get());
                result = (Result) reader.getCurrentValue(result);
                NavigableMap<byte[], byte[]> resultMap = result.getFamilyMap(Bytes.toBytes("d"));

                HBaseOutput hbaseOutput = new HBaseOutput(skey);
                resultMap.forEach((k, v) -> {
                    hbaseOutput.hbaseValue.put(Bytes.toString(k), Bytes.toString(v));
                });

                outputList.add(hbaseOutput);

                if(count % _batchSize == 0)
                {
                    persist(count, outputList);
                    outputList.clear();
                }
            }

            reader.close();

            persist(count, outputList);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void persist(long count, ArrayList<HBaseOutput> outputList) throws IOException {
        Gson gson = new Gson();
        BufferedWriter writer = new BufferedWriter(new FileWriter(_destinationPath + count + ".json"));
        for (HBaseOutput outputLine : outputList) {
            writer.write(gson.toJson(outputLine) + "\n");
        }
        writer.write(gson.toJson(outputList));
        writer.close();
    }
}
