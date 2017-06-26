package co.wecommit.unmanagedextension;


import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path("/common-actors/traversal")
public class CommonActorsTraversal {

    @Context GraphDatabaseService db;
    @Context Log log;

    private final ObjectMapper objectMapper;

    public static final RelationshipType ACTS_IN = RelationshipType.withName("ACTS_IN");

    private static final Label Actor = Label.label("Actor");
    private static final Label Movie = Label.label("Movie");

    private static final BidirectionalActsInExpander ActsInExpander = new BidirectionalActsInExpander();
    private static final InitialBranchState.State<Double> state = new InitialBranchState.State<>(0.0, 0.0);

    public CommonActorsTraversal(@Context GraphDatabaseService graphDb) {
        this.db = graphDb;
        this.objectMapper = new ObjectMapper();
    }

    @GET
//    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/{start}/{end}" )
    public Response commonActorsTraversal(@PathParam("start") String start, @PathParam("end") String end) {
        // TODO: Convert from string of names into array
        ArrayList<String> results = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            Node start_node = db.findNode(Movie, "title", start);
            Node end_node = db.findNode(Movie, "title", end);

            TraversalDescription traversal = db.traversalDescription()
                    .breadthFirst()
                    .expand(ActsInExpander, state)
                    .uniqueness(Uniqueness.NODE_PATH)
                    .evaluator(Evaluators.toDepth(2));

            BidirectionalTraversalDescription bidirectional = db.bidirectionalTraversalDescription()
                    .mirroredSides(traversal)
                    .collisionEvaluator(new CollisionEvaluator());


            log.info("Traverse from " + start_node.getId() + " : "+ start_node.getProperty("title"));
            log.info("Traverse to   " + end_node.getId() + " : "+ end_node.getProperty("title"));


            for (org.neo4j.graphdb.Path path : bidirectional.traverse(start_node, end_node)) {
                for (Node node : path.nodes()) {

                    if (node.hasLabel(Actor)) {
//                        results.add(node);
                        results.add((String) node.getProperty("name"));
                    }
                }
            }

            tx.success();
        }

        return Response.ok().entity(results.toString())/*&.type(MediaType.APPLICATION_JSON)*/.build();
    }



}
