package org.goldenorb.algorithms.pageRank;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.goldenorb.OrbRunner;
import org.goldenorb.conf.OrbConfiguration;
import org.goldenorb.types.message.DoubleMessage;

public class OrbPageRankJob extends OrbRunner {
  
  public static final String ALGORITHM = "pagerank";
	public static final String NUMVERTICES = "numvertices";
	public static final String MAXITERATIONS = "maxiterations";
	public static final String DAMPINGFACTOR = "boundaryfactor";
	
	public static final String USAGE = "mapred.input.dir=/home/user/input/ mapred.output.dir=/home/user/output/ goldenOrb.orb.requestedPartitions=3 goldenOrb.orb.reservedPartitions=0 " + NUMVERTICES  + "=1000 " + MAXITERATIONS  + "=10 " + DAMPINGFACTOR + "=0.85";

	private OrbConfiguration orbConf;

	public static void main(String[] args) {
		OrbPageRankJob oprj = new OrbPageRankJob();
		oprj.startJob(args);
	}

	public void startJob(String[] args) {
		orbConf = new OrbConfiguration(true);

		orbConf.setFileInputFormatClass(TextInputFormat.class);
		orbConf.setFileOutputFormatClass(TextOutputFormat.class);
		orbConf.setVertexClass(PageRankVertex.class);
		orbConf.setMessageClass(DoubleMessage.class);
		orbConf.setVertexInputFormatClass(PageRankVertexReader.class);
		orbConf.setVertexOutputFormatClass(PageRankVertexWriter.class);
		orbConf.setNumberOfMessageHandlers(10);
		orbConf.setNumberOfVertexThreads(10);
		
    try {
      parseArgs(orbConf, args, ALGORITHM);   
    } catch (Exception e) {
      printHelpMessage();
      System.exit(-1);
    }

    runJob(orbConf);
	}
}
