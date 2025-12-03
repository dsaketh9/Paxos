package paxos.Node;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import paxos.rpc.PaxosServiceGrpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PaxosServer {


    static final class Addr {
        final String host; final int port;
        Addr(String h, int p) { host = h; port = p; }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String nodeId = null;
        int port = -1;
        String peersStr = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id"    -> nodeId = args[++i];
                case "--port"  -> port   = Integer.parseInt(args[++i]);
                case "--peers" -> peersStr = args[++i];
            }
        }
        if (nodeId == null || port == -1) {
            System.err.println("Usage: --id <nodeId> --port <port> --peers <peer1:host:port,...>");
            System.exit(1);
        }


        Map<String, Addr> directory = new LinkedHashMap<>();


        Map<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peerStubs = new LinkedHashMap<>();

        directory.put(nodeId, new Addr("localhost", port));

        if (!peersStr.isEmpty()) {
            for (String peerAddress : peersStr.split(",")) {
                String[] parts = peerAddress.trim().split(":");
                if (parts.length != 3) continue;
                String peerId = parts[0];
                String host   = parts[1];
                int peerPort  = Integer.parseInt(parts[2]);


                directory.put(peerId, new Addr(host, peerPort));

                if (!peerId.equals(nodeId)) {
                    ManagedChannel ch = ManagedChannelBuilder.forAddress(host, peerPort)
                            .usePlaintext()
                            .build();
                    peerStubs.put(peerId, PaxosServiceGrpc.newBlockingStub(ch));
                }
            }
        }
        System.out.println("[" + nodeId + "] Connected to peers: " + peerStubs.keySet());


        Node paxosNode = new Node(nodeId, peerStubs, directory);
        PaxosNodeService paxosService = new PaxosNodeService(paxosNode);


        Runnable electionTimeoutTask = () -> {
            boolean shouldStartElection = paxosNode.onElectionTimeout();
            if (shouldStartElection) paxosNode.startElection();
        };
        paxosNode.setElectionTimeoutTask(electionTimeoutTask);
        paxosNode.resetElectionTimer();

        Server server = NettyServerBuilder.forPort(port)
                .addService(paxosService)
                .build();

        server.start();
        paxosNode.resetElectionTimer();
        paxosNode.scheduler.schedule(paxosNode::startElection, 350, java.util.concurrent.TimeUnit.MILLISECONDS);
        System.out.println("[" + nodeId + "] Server started, listening on " + port);
        System.out.println("Enter commands (printdb, printlog, printview, printstatus <seq>, sleep, wake, exit)...");


        final String finalNodeId = nodeId;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** ["+ finalNodeId + "] Shutting down gRPC server...");
            try { server.shutdown().awaitTermination(30, TimeUnit.SECONDS); }
            catch (InterruptedException e) { e.printStackTrace(System.err); }
            System.err.println("*** ["+ finalNodeId + "] Server shut down.");
        }));

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (line.equals("printdb"))        paxosNode.printDB();
                else if (line.equals("printlog"))  paxosNode.printLog();
                else if (line.equals("printview")) paxosNode.printView();
                else if (line.equals("printstatusall")) paxosNode.printStatusAll();
                else if (line.startsWith("printstatus")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length > 1) paxosNode.printStatus(Long.parseLong(parts[1]));
                    else System.out.println("Usage: printstatus <sequenceNumber> or printstatusall");
                } else if (line.equals("sleep"))   paxosNode.pauseNode();
                else if (line.equals("wake"))      paxosNode.resumeNode();
                else if (line.equals("exit"))      break;
                else System.out.println("Unknown command. Available: printdb, printlog, printstatusall, printstatus <seq>, printview, sleep, wake, exit");
            }
        }

        server.awaitTermination();
    }
}
