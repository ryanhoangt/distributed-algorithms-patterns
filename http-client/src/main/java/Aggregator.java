import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {

    private WebClient webClient;

    public Aggregator() {
        this.webClient = new WebClient();
    }

    public List<String> sendTasksToWorkers(List<String> workerAddresses, List<String> tasks) {
        CompletableFuture<String>[] futures = new CompletableFuture[workerAddresses.size()];

        for (int i = 0; i < workerAddresses.size(); i++) {
            String workerAddress = workerAddresses.get(i);
            String task = tasks.get(i);
            byte[] requestPayload = task.getBytes();

            futures[i] = webClient.sendTask(workerAddress, requestPayload);

            // for testing purpose
            try {
                /**
                 * We can reuse the same connection only if the entire transaction that occupied
                 * that connection previously has already been completed.
                 * Allow enough time for the response to the first request to arrive at a client
                 */
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        List<String> results = Stream.of(futures)
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        return results;
    }

}
