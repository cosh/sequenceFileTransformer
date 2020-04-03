package com.cosh.transformer;

import com.cosh.transformer.sequencefile.SequenceFileTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SequenceFileTransformerApplication implements CommandLineRunner {

	@Autowired
	SequenceFileTransformer transformer;

	private static Logger LOG = LoggerFactory
			.getLogger(SequenceFileTransformerApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SequenceFileTransformerApplication.class, args);
	}

	@Override
	public void run(String... args) {
		LOG.info("EXECUTING : command line runner");
		transformer.transformSequenceFile();
	}
}
