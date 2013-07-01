package net.pvardanega.millionsongsdataset;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ArtistsReader {

    private static String TXT_SEPARATOR = "<SEP>";
    private static String CSV_SEPARATOR = ",";

    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Missing parameter: elastic search host ");
            System.exit(1);
        }

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(args[0], 9300));

        // Empty the index
        client.prepareDelete("yawyl", "artists", "").execute().actionGet();

        // Retrieve artists' similarities
        List<String> similarities = Files.readAllLines(Paths.get("src/main/resources/subset_artist_similarity.csv"),
                Charset.defaultCharset());
        Map<String, List<String>> artistsSimilarities = new TreeMap<>();
        for(String similarity: similarities) {
            String[] fields = similarity.split(CSV_SEPARATOR);
            if (!artistsSimilarities.containsKey(fields[0])) {
                artistsSimilarities.put(fields[0], new ArrayList<String>());
            }
            artistsSimilarities.get(fields[0]).add(fields[1]);
        }

        // Retrieve artists' terms
        List<String> terms = Files.readAllLines(Paths.get("src/main/resources/subset_artist_term.csv"),
                Charset.defaultCharset());
        Map<String, List<String>> artistsTerms = new TreeMap<>();
        for(String term: terms) {
            String[] fields = term.split(CSV_SEPARATOR);
            String artist_id = fields[0];
            String term_artist_id = fields[1].replaceAll("\"", "");
            if (!artistsTerms.containsKey(artist_id)) {
                artistsTerms.put(artist_id, new ArrayList<String>());
            }
            artistsTerms.get(artist_id).add(term_artist_id);
        }

        // Retrieve artists' terms
        List<String> mbtags = Files.readAllLines(Paths.get("src/main/resources/subset_artist_mbtag.csv"),
                Charset.defaultCharset());
        Map<String, List<String>> artistsMbtags = new TreeMap<>();
        for(String mbtag: mbtags) {
            String[] fields = mbtag.split(CSV_SEPARATOR);
            String artist_id = fields[0];
            String mbtag_artist_id = fields[1].replaceAll("\"", "");
            if (!artistsMbtags.containsKey(artist_id)) {
                artistsMbtags.put(artist_id, new ArrayList<String>());
            }
            artistsMbtags.get(artist_id).add(mbtag_artist_id);
        }

        // Retrieve artists with locations
        List<String> locationLines = Files.readAllLines(Paths.get("src/main/resources/subset_artist_location.txt"),
                Charset.defaultCharset());
        Map<String, String> artistsWithLocations = new TreeMap<>();
        for(String locationLine: locationLines) {
            String[] fields = locationLine.split(TXT_SEPARATOR);
            artistsWithLocations.put(fields[0], fields[1] + TXT_SEPARATOR + fields[2]);
        }

        // Push artists to elastic search
        List<String> artistLines = Files.readAllLines(Paths.get("src/main/resources/subset_unique_artists.txt"),
                Charset.defaultCharset());
        String[] locations;
        for(String artistLine: artistLines) {
            String[] fields = artistLine.split(TXT_SEPARATOR);
            String id = fields[0];
            String name = fields[3];
            if (id != null && id.length() > 0 && name != null && name.length() > 0) {
                locations = artistsWithLocations.containsKey(name) ?
                        artistsWithLocations.get(name).split(TXT_SEPARATOR) : null;
                try {
                    client.prepareIndex("yawyl", "artists", id)
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("artist_id", id)
                                    .field("name", name)
                                    .field("lattitude", locations == null ? null : locations[0])
                                    .field("longitude", locations == null ? null : locations[1])
                                    .field("similarity_id", artistsSimilarities.get(id) == null ? new
                                            ArrayList() : artistsSimilarities.get(id))
                                    .field("mbtag", artistsMbtags.get(id) == null ? new ArrayList() : artistsMbtags.get(id))
                                    .field("term", artistsTerms.get(id) == null ? new ArrayList() : artistsTerms.get(id))
                                    .endObject()
                            )
                            .execute().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }


        System.out.println("### LOGS ###");
        CountResponse all = client.prepareCount("yawyl").execute().actionGet();
        System.out.println(all.getCount() + " artists in the index");

        client.close();
    }
}
