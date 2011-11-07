package org.goldenorb.algorithms.pageRank;


import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.goldenorb.Edge;
import org.goldenorb.io.input.VertexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertexReader extends VertexBuilder<PageRankVertex,LongWritable,Text>{

	private final static Logger logger = LoggerFactory.getLogger(PageRankVertexReader.class);
	
  @Override
	public PageRankVertex buildVertex(LongWritable key, Text value) {
		
	  String[] values = value.toString().split("\t");
		ArrayList<Edge<IntWritable>> edgeCollection = new ArrayList<Edge<IntWritable>>();
		for(int i=1; i < values.length; i++){
			Edge<IntWritable> edge = new Edge<IntWritable>(values[i].trim(), new IntWritable(1));
			edgeCollection.add(edge);
		}
		
		//String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges
		PageRankVertex vertex = new PageRankVertex(values[0].trim(), new DoubleWritable(0.0), edgeCollection);
		
		return vertex;
	}

}
