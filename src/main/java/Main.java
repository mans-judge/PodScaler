import io.kubernetes.client.ProtoClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.proto.V1Apps.Deployment;
import io.kubernetes.client.proto.V1Apps.DeploymentSpec;
import io.kubernetes.client.util.ClientBuilder;
import okhttp3.Call;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
    public static void main(String[] args) {
        //check every 1 minute
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    scaleDeployment();
                } catch (IOException | ApiException e) {
                    e.printStackTrace();
                }
            }
        },0, 60 * 1000);
    }

    private static void scaleDeployment() throws IOException, ApiException {
        String deploymentPath = "/api/v1/namespaces/default/deployments/judger";
        int maxPodQueueLength = 5;

        //connect to cluster client
        ApiClient client = ClientBuilder.cluster().build();
        Configuration.setDefaultApiClient(client);
        ProtoClient protoClient = new ProtoClient(client);

        //TODO: get number of submissions from backend
        Call call = client.buildCall("backend:8080", "GET", null,
                null,
                null,
                null,
                null,
                null,
                null,
                null );
        int submissions = (int) client.execute(call, Integer.TYPE).getData();

        //update number of replicas
        DeploymentSpec currentSpec = ((Deployment) protoClient
                .get(Deployment.newBuilder(), deploymentPath).object)
                .getSpec();
        Deployment updated = Deployment.newBuilder()
                                       .setSpec(DeploymentSpec
                                               .newBuilder(currentSpec)
                                               .setReplicas(submissions / maxPodQueueLength + 1))
                                       .build();
        protoClient.update(updated, deploymentPath, "v1", "Deployment");
    }
}
