package bfs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IterateBFSReducer extends Reducer<Text, BFSNode, Text, BFSNode> {

	// For emitting estimated distances to the outgoing nodes
	private BFSNode resultNode = null;
	
	
	public void reduce(Text nid, Iterable<BFSNode> values,	Context context) throws IOException, InterruptedException {

		int min = Integer.MAX_VALUE;
		
		
		for (BFSNode node : values) {
		
			// complete code here in order to estimate the minimum distance found among the node neighbours
			
			
			
			//we set the result node as the one emitted from the original node in the mapper - as it needs no more 
			if(!node.getId().equals(BFSNode.DISTANCE_INFO))
				resultNode = node;
		}
		
		if(min<Integer.MAX_VALUE)
			context.getCounter(IterateBFS.Counters.ReachableNodesAtReduce).increment(1);
		
		resultNode.setDistance(min);
		
		
		context.write(nid, resultNode);

		
		
	}

}