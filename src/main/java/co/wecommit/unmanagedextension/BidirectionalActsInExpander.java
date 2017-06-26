package co.wecommit.unmanagedextension;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;


public class BidirectionalActsInExpander implements PathExpander<Double> {

    public static final RelationshipType ACTS_IN = RelationshipType.withName("ACTS_IN");

    @Override
    public  Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
        return path.endNode().getRelationships(Direction.INCOMING, ACTS_IN);
    }

    @Override
    public PathExpander<Double> reverse() {
        return new PathExpander<Double>() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState<Double> state) {
                return path.endNode().getRelationships(Direction.INCOMING, ACTS_IN);
            }

            @Override
            public PathExpander<Double> reverse() {
                return null;
            }
        };
    }

}
