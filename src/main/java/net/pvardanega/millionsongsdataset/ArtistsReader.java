package net.pvardanega.millionsongsdataset;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ArtistsReader {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Usage: ./run.sh elasticsearch-host.com");
            System.exit(1);
        }

        List<String> lines = Files.readAllLines(Paths.get("src/main/resources/subset_unique_artists.txt"),
                                                Charset.defaultCharset());

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(args[0], 9300));

        for (String line : lines) {
            String[] fields = line.split("<SEP>");
            String id = fields[1];
            String name = fields[3];
            if (id != null && id.length() > 0 && name != null && name.length() > 0) {
                client.prepareIndex("yawyl", "artists", id)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("artist_id", id)
                                .field("name", name)
                                .endObject()
                        )
                        .execute();
            }
        }

        client.close();
    }

}
