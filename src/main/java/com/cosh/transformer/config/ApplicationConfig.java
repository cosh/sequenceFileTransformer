package com.cosh.transformer.config;

import com.cosh.transformer.sequencefile.SequenceFileTransformer;
import com.cosh.transformer.sequencefile.SequenceFileToJsonTransformerImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {

    @Value("${sourceFile}")
    private String _sourceFile;

    @Value("${destinationPath}")
    private String _destinationPath;

    @Value("${batchSize}")
    private long _batchSize = 1000000;

    @Bean
    public SequenceFileTransformer sequenceFileReader()
    {
        return new SequenceFileToJsonTransformerImpl(_sourceFile, _destinationPath, _batchSize);
    }
}
