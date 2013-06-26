package net.pvardanega.hdf5;

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
     * curl -X GET http://ec2-54-229-70-224.eu-west-1.compute.amazonaws.com:9200/yawyl/artists/_count
     * curl -X DELETE http://ec2-54-229-70-224.eu-west-1.compute.amazonaws.com:9200/yawyl/artists/
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("src/main/resources/subset_unique_artists.txt"),
                                                Charset.defaultCharset());

        Client client = new TransportClient()
                .addTransportAddress(
                        new InetSocketTransportAddress("ec2-54-229-70-224.eu-west-1.compute.amazonaws.com",9300));

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
