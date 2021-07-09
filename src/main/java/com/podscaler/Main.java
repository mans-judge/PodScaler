package com.podscaler;
import io.kubernetes.client.ProtoClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.proto.V1Apps.Deployment;
import io.kubernetes.client.proto.V1Apps.DeploymentSpec;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.proto.V1.Namespace;
import io.kubernetes.client.proto.V1.NamespaceSpec;
import io.kubernetes.client.proto.V1.Pod;
import io.kubernetes.client.proto.V1.PodList;
import io.kubernetes.client.ProtoClient.ObjectOrStatus;
import okhttp3.Call;
import okhttp3.Call;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.net.*;

public class Main {
   public static int[] judge_history = {0, 0, 0, 0, 0};
   public static int[] compiler_history = {0, 0, 0, 0, 0};
   public static int[] pending_submissions_history = {0, 0, 0, 0, 0};
   public static int[] pending_compilations_history = {0, 0, 0, 0, 0};
   public static int[] compilers_history = {1, 1, 1, 1, 1};
   public static int[] judgers_history = {1, 1, 1, 1, 1};
   public static String get(String urlToRead) throws Exception {
      StringBuilder result = new StringBuilder();
      URL url = new URL(urlToRead);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
     conn.setRequestMethod("GET");
       try (BufferedReader reader = new BufferedReader(
                   new InputStreamReader(conn.getInputStream()))) {
           for (String line; (line = reader.readLine()) != null; ) {
               result.append(line);
           }
       }
       return result.toString();
    }
    public static void main(String[] args) {
        //check every 1 minute
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    scaleJudgerDeployment();
                    scaleCompilerDeployment();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },0, 60 * 1000);
    }

    private static void scaleJudgerDeployment() throws IOException, ApiException, Exception {
        for(int i = 0;i < 4;i++)
            Main.judge_history[i] = Main.judge_history[i + 1];
        for(int i = 0;i < 4;i++)
            Main.judgers_history[i] = Main.judgers_history[i + 1];
        for(int i = 0;i < 4;i++)
            Main.pending_submissions_history[i] = Main.pending_submissions_history[i + 1];
        String deploymentPath = "/apis/apps/v1/namespaces/default/deployments/judger";
        int maxPodQueueLength = 5;
        int maxPods = 90;

        //connect to cluster client
        ApiClient client = ClientBuilder.cluster().build();
        Configuration.setDefaultApiClient(client);
        ProtoClient protoClient = new ProtoClient(client);

        ObjectOrStatus<PodList> list = protoClient.list(PodList.newBuilder(), "/api/v1/namespaces/default/pods");

        //TODO: get number of submissions from backend
        int submissions = (int) Integer.parseInt(get("http://backend:7000/api/v1/submissions/pending"));
        Deployment deployment = (Deployment) protoClient
                .get(Deployment.newBuilder(), deploymentPath).object;
        Main.pending_submissions_history[4] = submissions;
        double rate = (Main.pending_submissions_history[4] - Main.pending_submissions_history[3]);
        int npods = Main.judgers_history[3] + (int)((rate + 5) / 5);
        npods = Math.min(npods, Main.pending_submissions_history[4] + 1);
        npods = Math.max(npods, 1);
        npods = Math.max(npods, Main.judgers_history[3] - 3);
        Main.judge_history[4] = npods;
        int nval = 1;
        for(int i = 0;i < 5;i++)
            nval = Math.max(nval, Main.judge_history[i]);
        nval = Math.min(nval, maxPods);
        Main.judgers_history[4] = nval;
        //update number of replicas
        DeploymentSpec currentSpec = deployment.getSpec();
        int replicas = currentSpec.getReplicas();
        if(replicas > nval && Math.abs(replicas - nval) < 3){
            nval = replicas;
        }else if(replicas < nval && Math.abs(replicas - nval) <= 5){
            nval = replicas + 6;
        }
        Deployment updated = Deployment.newBuilder(deployment)
                                       .setSpec(DeploymentSpec
                                               .newBuilder(currentSpec)
                                               .setReplicas(nval))
                                       .build();
        System.out.printf("submissions: %d%ncurrent scaling to: %d%nreplicas: %d%nactual scaling to: %d%n", submissions, Main.judge_history[4], updated.getSpec().getReplicas(), nval);
        ObjectOrStatus<Deployment> dp = protoClient.update(updated, deploymentPath, "apps/v1", "Deployment");
        System.out.println(dp.status);
    }

    private static void scaleCompilerDeployment() throws IOException, ApiException, Exception {
        for(int i = 0;i < 4;i++)
            Main.compiler_history[i] = Main.compiler_history[i + 1];
        for(int i = 0;i < 4;i++)
            Main.compilers_history[i] = Main.compilers_history[i + 1];
        for(int i = 0;i < 4;i++)
            Main.pending_compilations_history[i] = Main.pending_compilations_history[i + 1];
        String deploymentPath = "/apis/apps/v1/namespaces/default/deployments/compiler";
        int maxPodQueueLength = 20;
        int maxPods = 60;
        //connect to cluster client
        ApiClient client = ClientBuilder.cluster().build();
        Configuration.setDefaultApiClient(client);
        ProtoClient protoClient = new ProtoClient(client);

        ObjectOrStatus<PodList> list = protoClient.list(PodList.newBuilder(), "/api/v1/namespaces/default/pods");

        //TODO: get number of submissions from backend
        int submissions = (int) Integer.parseInt(get("http://backend:7000/api/v1/submissions/pending_compilations"));
        Deployment deployment = (Deployment) protoClient
                .get(Deployment.newBuilder(), deploymentPath).object;
        Main.pending_compilations_history[4] = submissions;
        double rate = (Main.pending_compilations_history[4] - Main.pending_compilations_history[3]);
        int npods = Main.compilers_history[3] + (int)((rate + 5) / 5);
        npods = Math.min(npods, Main.pending_compilations_history[4] + 1);
        npods = Math.max(npods, 1);
        Main.compiler_history[4] = npods;
        npods = Math.max(npods, Main.compilers_history[3] - 3);

        int nval = 1;
        for(int i = 0;i < 5;i++)
            nval = Math.max(nval, Main.compiler_history[i]);
        nval = Math.min(nval, maxPods);
        Main.compilers_history[4] = nval;
        //update number of replicas
        DeploymentSpec currentSpec = deployment.getSpec();
        int replicas = currentSpec.getReplicas();
        if(replicas > nval && Math.abs(replicas - nval) < 3){
            nval = replicas;
        }else if(replicas < nval && Math.abs(replicas - nval) <= 5){
            nval = replicas + 6;
        }
        Deployment updated = Deployment.newBuilder(deployment)
                                       .setSpec(DeploymentSpec
                                               .newBuilder(currentSpec)
                                               .setReplicas(nval))
                                       .build();
        System.out.printf("compilations: %d%ncurrent scaling to: %d%nreplicas: %d%nactual scaling to: %d%n", submissions, Main.compiler_history[4], updated.getSpec().getReplicas(), nval);
        ObjectOrStatus<Deployment> dp = protoClient.update(updated, deploymentPath, "apps/v1", "Deployment");
        System.out.println(dp.status);
    }
}
