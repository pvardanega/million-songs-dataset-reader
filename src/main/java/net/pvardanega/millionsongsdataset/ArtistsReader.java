package net.pvardanega.millionsongsdataset;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ArtistsReader {

    private static String SEPARATOR = "<SEP>";

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Missing parameter: elastic search host ");
            System.exit(1);
        }

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(args[0], 9300));

        // Retrieve artists with locations
        List<String> locationLines = Files.readAllLines(Paths.get("src/main/resources/subset_artist_location.txt"),
                Charset.defaultCharset());
        Map<String, String> artistsWithLocations = new HashMap<>();
        for(String locationLine: locationLines) {
            String[] fields = locationLine.split(SEPARATOR);
            artistsWithLocations.put(fields[4], fields[1] + SEPARATOR + fields[2]);
        }

        // Push artists to elastic search
        List<String> artistLines = Files.readAllLines(Paths.get("src/main/resources/subset_unique_artists.txt"),
                Charset.defaultCharset());
        int indice = 0;
        String[] locations;
        for(String artistLine: artistLines) {
            String[] fields = artistLine.split(SEPARATOR);
            String id = fields[1];
            String name = fields[3];
            if (id != null && id.length() > 0 && name != null && name.length() > 0) {
                locations = artistsWithLocations.containsKey(name) ?
                        artistsWithLocations.get(name).split(SEPARATOR) : null;
                try {
                    client.prepareIndex("yawyl", "artists", id)
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("artist_id", id)
                                    .field("name", name)
                                    .field("lattitude", locations == null ? null : locations[0])
                                    .field("longitude", locations == null ? null : locations[1])
                                    .endObject()
                            )
                            .execute().get();
                    indice++;
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        client.close();
        System.out.println(indice + " artists added");
    }

}
