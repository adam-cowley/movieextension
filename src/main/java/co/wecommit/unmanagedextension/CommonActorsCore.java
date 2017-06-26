package co.wecommit.unmanagedextension;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStream;

@Path("/common-actors/core")
public class CommonActorsCore {

    @Context
    private final GraphDatabaseService db;
    private final ObjectMapper objectMapper;

    private static final RelationshipType ACTS_IN = RelationshipType.withName("ACTS_IN");
    private static final Label Movie = Label.label("Movie");

    public CommonActorsCore(@Context GraphDatabaseService graphDb) {
        this.db = graphDb;
        this.objectMapper = new ObjectMapper();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{start}/{end}")
    public Response getResponse(@PathParam("start") String start, @PathParam("end") String end) {
        StreamingOutput stream = (OutputStream output) -> {
            JsonGenerator json = objectMapper.getJsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);

            json.writeStartArray();

            try (Transaction tx = db.beginTx()) {
                Node start_node = db.findNode(Movie, "title", start);
                Node end_node = db.findNode(Movie, "title", end);

                for (Relationship rel : start_node.getRelationships(Direction.INCOMING, ACTS_IN)) {
                    Node actor = rel.getStartNode();

                    for (Relationship mutualrel : actor.getRelationships(Direction.OUTGOING, ACTS_IN)) {
                        Node other_movie = mutualrel.getEndNode();

                        if (other_movie.equals(end_node)) {
                            json.writeStartObject();

                            json.writeFieldName("_id");
                            json.writeNumber(actor.getId());

                            json.writeFieldName("name");
                            json.writeString((String) actor.getProperty("name"));

                            json.writeEndObject();
                        }
                    }
                }

                tx.success();
            }

            json.writeEndArray();
            json.flush();
            json.close();
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }
}