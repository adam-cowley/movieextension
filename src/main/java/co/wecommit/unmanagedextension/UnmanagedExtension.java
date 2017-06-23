package co.wecommit.unmanagedextension;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;


@Path("/movies")
public class UnmanagedExtension {

    @Context GraphDatabaseService db;
    private final ObjectMapper objectMapper;


    private static final RelationshipType ACTED_IN = RelationshipType.withName("ACTED_IN");
    private static final Label Person = Label.label("Person");
    private static final Label Movie = Label.label("Movie");


    public UnmanagedExtension( @Context GraphDatabaseService graphDb ) {
        this.db = graphDb;
        this.objectMapper = new ObjectMapper();
    }


    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/common-actors/{first}/{second}" )
    public Response commonActorsTraversal(@PathParam( "first" ) String first, @PathParam( "second" ) String second) {
        StreamingOutput stream = (OutputStream output) -> {
            Map params = new HashMap();

            params.put("first", first);
            params.put("second", second);

            JsonGenerator json = objectMapper.getJsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);

            json.writeStartArray();

            try (Transaction tx = db.beginTx()) {
                Node movie = db.findNode(Movie, "title", first );

                json.writeStartArray();

                json.writeFieldName("_id");
                json.writeNumber(movie.getId());

                json.writeEndObject();


                tx.success();

            }

            json.writeEndArray();
            json.flush();
            json.close();
        };

        return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/common-actors-cypher/{first}/{second}" )
    public Response commonActorsCypher( @PathParam( "first" ) String first, @PathParam( "second" ) String second ) {
        Map params = new HashMap();

        params.put("first", first);
        params.put("second", second);

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                JsonGenerator json = objectMapper.getJsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);

                json.writeStartArray();

                try (Transaction tx = db.beginTx(); Result result = db.execute(commonActorsCypherQuery(), params)) {
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();

                        Node actor = (Node) row.get("actor");

                        json.writeStartObject();

                        json.writeFieldName("_id");
                        json.writeNumber(actor.getId());

                        json.writeFieldName("name");
                        json.writeString(actor.getProperty("name").toString());


                        json.writeEndObject();
                    }
                    tx.success();
                }

                json.writeEndArray();
                json.flush();
                json.close();
            }
        };

        // Do stuff with the database
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    private String commonActorsCypherQuery() {
        return "MATCH (first:Movie)<-[:ACTS_IN]-(actor:Actor)-[:ACTS_IN]->(second:Movie)" +
                "WHERE first.title = {first} AND second.title = {second}" +
                "MATCH (actor)-[r:ACTS_IN]->(m:Movie) return actor, collect({rel:r, movie:m}) as roles";
    }

}