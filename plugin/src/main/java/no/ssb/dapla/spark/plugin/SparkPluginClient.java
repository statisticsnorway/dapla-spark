package no.ssb.dapla.spark.plugin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.ssb.dapla.spark.protobuf.HelloRequest;
import no.ssb.dapla.spark.protobuf.HelloResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;

public class SparkPluginClient {

    private final SparkPluginServiceGrpc.SparkPluginServiceBlockingStub pluginClient;

    public SparkPluginClient(String host, int port) {
        // "host.docker.internal"
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port);
        channelBuilder.usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        pluginClient = SparkPluginServiceGrpc.newBlockingStub(channel);
    }

    void sayHelloToServer(String message) {
        HelloResponse helloFromClient = pluginClient.sayHello(HelloRequest.newBuilder().setGreeting(message).build());

        System.out.println(helloFromClient.getReply());
    }

}
