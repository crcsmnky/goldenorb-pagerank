package org.goldenorb.algorithms.pageRank;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.goldenorb.Edge;
import org.goldenorb.Vertex;
import org.goldenorb.types.message.DoubleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankVertex extends Vertex<DoubleWritable,IntWritable,DoubleMessage> {
  
  private static final String NUMVERTICES = OrbPageRankJob.ALGORITHM + "." + OrbPageRankJob.NUMVERTICES;
  private static final String MAXITERATIONS = OrbPageRankJob.ALGORITHM + "." + OrbPageRankJob.MAXITERATIONS;
  private static final String DAMPINGFACTOR = OrbPageRankJob.ALGORITHM + "." + OrbPageRankJob.DAMPINGFACTOR;
  
  private final static Logger logger = LoggerFactory.getLogger(PageRankVertex.class);
  
  // this should get passed in by the job
  int numVertices = 1000;
  int maxIterations = 10;
  double dampingFactor = 0.85;
  
  int outgoingEdgeCount = 0;
  
  public PageRankVertex() {
    super(DoubleWritable.class, IntWritable.class, DoubleMessage.class);
  }
  
  public PageRankVertex(String _vertexID, DoubleWritable _value, List<Edge<IntWritable>> _edges) {
    super(_vertexID, _value, _edges);
  }
  
  @Override
  public void compute(Collection<DoubleMessage> messages) {
    double _sum = 0.0;
    double _outgoing = 0.0;
    
    // on the first step, set the vertex's initial value
    if (super.superStep() == 1) {
      initParameters();
      _sum = 1.0 / (double) numVertices;
    }
    
    if (super.superStep() <= maxIterations) {
      for (DoubleMessage m : messages) {
        _sum += m.get();
      }
      _outgoing = computeOutgoingRank(_sum);
      
      for (Edge<IntWritable> e : getEdges()) {
        sendMessage(new DoubleMessage(e.getDestinationVertex(), new DoubleWritable(_outgoing)));
      }
    }
    
    this.voteToHalt();
  }
  
  private double computeOutgoingRank(double sum) {
    double value = ((1.0 - dampingFactor) / (double) numVertices) + (dampingFactor * sum);
    super.setValue(new DoubleWritable(value));
    double out = value / (double) outgoingEdgeCount;
    
    return out;
  }
  
  private void initParameters() {
    try {
      maxIterations = Integer.parseInt(super.getOci().getOrbProperty(MAXITERATIONS));
    } catch (Exception e) {
      logger.debug("Max Iterations not set");
      maxIterations = 10;
    }
    
    try {
      numVertices = Integer.parseInt(super.getOci().getOrbProperty(NUMVERTICES));
    } catch (Exception e) {
      logger.debug("Total Pages not set");
      numVertices = 1000;
    }
    
    try {
      dampingFactor = Double.parseDouble(super.getOci().getOrbProperty(DAMPINGFACTOR));
    } catch (Exception e) {
      logger.debug("Damping Factor not set");
      dampingFactor = 0.85;
    }
    
    outgoingEdgeCount = super.getEdges().size() == 0 ? numVertices : super.getEdges().size();
  }
  
  public double getPageRank() {
    return super.getValue().get();
  }
  
  @Override
  public String toString() {
    return "\"PageRank\":\"" + super.getValue().get() + "\"";
  }
}
