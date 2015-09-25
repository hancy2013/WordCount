package com.microstrategy.wordcount;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class WordCountOptions {
	
	@Option(name = "--inputFile", required = true, usage = "HDFS file path that holds input dataset")
	public String inputFile;
	
	@Option(name = "--outputFile", required = true, usage = "HDFS file path that holds output file path")
	public String outputFile;
	
	public void parse(String[] args) throws CmdLineException {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
		} catch(CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			throw e;
		}
	}

}
