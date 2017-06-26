package co.wecommit.unmanagedextension;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


public class CommonActorsProcedure {

    @Context
    public GraphDatabaseService db;

    private static final RelationshipType ACTS_IN = RelationshipType.withName("ACTS_IN");

    public static class ActorResult {
        public Node actor;

        public ActorResult(Node actor) {
            this.actor = actor;
        }
    }


    @Procedure(value="movies.commonActorsCypher", mode = Mode.READ)
    @Description("Find common actors in two movies by their movie titles")
    public Stream<ActorResult> commonActors(@Name("start") String start, @Name("end") String end){
        Map params = new HashMap<String, String>();
        params.put("start", start);
        params.put("end", end);

        ArrayList<ActorResult> results = new ArrayList<>();

        try (Transaction tx  = db.beginTx(); Result result = db.execute(commonActorsCypherQuery(), params)) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();

                ActorResult actor = new ActorResult((Node) row.get("actor"));

                results.add(actor);
            }

            tx.success();
        }

        return results.stream();
    }

    private String commonActorsCypherQuery() {
        return "MATCH (start:Movie)<-[:ACTS_IN]-(actor:Actor)-[:ACTS_IN]->(end:Movie)" +
                "WHERE start.title = {start} AND end.title = {end}" +
                "MATCH (actor)-[r:ACTS_IN]->(m:Movie) return actor, collect({rel:r, movie:m}) as roles";
    }

    @Procedure(value="movies.commonBetween", mode = Mode.READ)
    @Description("Get all common actors between two movie nodes")
    public Stream<ActorResult> commonActorsCore(@Name("start") Node start, @Name("end") Node end) {
        ArrayList<ActorResult> results = new ArrayList<>();

        for (Relationship rel : start.getRelationships(Direction.INCOMING, ACTS_IN)) {
            Node actor = rel.getStartNode();

            for (Relationship mutual : actor.getRelationships(Direction.OUTGOING, ACTS_IN)) {
                Node other_movie = mutual.getEndNode();

                if (end.equals(other_movie)) {
                    results.add(new ActorResult(actor));
                }
            }
        }

        return results.stream();
    }
}
