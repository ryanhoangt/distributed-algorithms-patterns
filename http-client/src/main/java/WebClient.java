import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class WebClient {
    private HttpClient client;

    public WebClient() {
        this.client = HttpClient.newBuilder()
                /**
                 * 1) use HTTP_2 version, each request will attempt to upgrade
                 * to HTTP 2 by setting specific headers. If the server does not
                 * support HTTP 2, these headers will be ignored.
                 * 2) HTTP 2 allows us to send a second request to the server BEFORE
                 * a response to the first request has been received. (This is totally
                 * different to Connection pooling aka Keep-Alive header concept).
                 */
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    public CompletableFuture<String> sendTask(String url, byte[] requestPayload) {
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestPayload))
                .uri(URI.create(url))
                .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString()) // receive response body as String
                .thenApply(HttpResponse::body); // get the reponse's body, ignore header
    }
}
