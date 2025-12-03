package paxos.Client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import paxos.rpc.*;
import com.google.protobuf.Empty;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import paxos.Node.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;


public class ClientMain {



    static class NodeInfo implements AutoCloseable {
        final String id;
        final String host;
        final int port;
        final ManagedChannel ch;
        final PaxosServiceGrpc.PaxosServiceBlockingStub stub;
        private final AtomicBoolean isShutdown = new AtomicBoolean(false);

        NodeInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            this.stub = PaxosServiceGrpc.newBlockingStub(ch);
        }
        
        @Override
        public void close() {
            if (isShutdown.compareAndSet(false, true)) {
                ch.shutdown();
                try {
                    if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                        ch.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ch.shutdownNow();
                }
            }
        }
    }

    static final Map<String, NodeInfo> NODES = new LinkedHashMap<>();

    static final AtomicReference<CountDownLatch> STEP_LATCH = new AtomicReference<>();


    static final boolean CLIENT_DEFER_ENABLED =
            "1".equals(System.getenv("CLIENT_DEFER_ENABLED")) ||
            Boolean.parseBoolean(System.getProperty("client.defer.enabled", "true"));
    static final ConcurrentMap<String, java.util.concurrent.ConcurrentLinkedDeque<Transaction>> CARRYOVER = new ConcurrentHashMap<>();

    static void enqueueCarryover(Transaction t) {
        if (!CLIENT_DEFER_ENABLED || t == null) return;
        CARRYOVER.computeIfAbsent(t.getClientId(), k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).addLast(t);
        // System.out.printf("[CLIENT] deferred txn for next set: client=%s ts=%d %s%n", t.getClientId(), t.getTimestamp(), fmtTxn(t));
    }

    static List<Transaction> drainCarryover() {
        List<Transaction> out = new ArrayList<>();
        if (!CLIENT_DEFER_ENABLED) return out;
        List<String> keys = new ArrayList<>(CARRYOVER.keySet());
        java.util.Collections.sort(keys);
        for (String k : keys) {
            var dq = CARRYOVER.get(k);
            if (dq == null) continue;
            Transaction t;
            while ((t = dq.pollFirst()) != null) out.add(t);
        }
        return out;
    }



    static class LeaderTracker {
        volatile int leaderId = -1;
        volatile long lastBallotRound = -1;
        synchronized void learnFromBallot(Ballot b) {
            if (b == null) return;
            if (b.getRound() > lastBallotRound) {
                leaderId = b.getNodeId();
                lastBallotRound = b.getRound();
            }

        }
        synchronized int current() { return leaderId; }
        synchronized void forget() { leaderId = -1;
        lastBallotRound = -1;
        }
    }

    static final LeaderTracker LEADER = new LeaderTracker();

    static final ConcurrentMap<Long, CompletableFuture<Void>> INFLIGHT = new ConcurrentHashMap<>();
    //list og inactive nodes
    static final Set<String> BLACKLIST = ConcurrentHashMap.newKeySet();

    static class ClientWorker implements Runnable {
        static final int    MAX_ATTEMPTS       = 10;
        static final int    BASE_BACKOFF_MS    = 50;
        static final long   ELECTION_GRACE_MS  =
                paxos.Node.Node.ELECTION_TIMEOUT_MAX_MS
                        + paxos.Node.Node.PREPARE_RPC_DEADLINE_MS
                        + paxos.Node.Node.VIEW_RPC_DEADLINE_MS;

        final BlockingQueue<Transaction> queue;
        final String name;
        final Random rnd = new Random();
        ClientWorker(String name, BlockingQueue<Transaction> q) { this.name = name; this.queue = q; }

        static void runSet(TestSet set, BlockingQueue<Transaction> q) throws Exception {
            System.out.println("=== START SET " + set.setNumber + " live=" + set.liveNodes + " ===");
            applyLiveNodes(set.liveNodes);
            BLACKLIST.clear();
            LEADER.forget();

            if (CLIENT_DEFER_ENABLED) {
                // int carryPending = 0;
                // for (var dq : CARRYOVER.values()) { if (dq != null) carryPending += dq.size(); }
                // System.out.println("[SET " + set.setNumber + "] carryover pending before drain: " + carryPending);
            }

            if (CLIENT_DEFER_ENABLED) {
                List<Transaction> carry = drainCarryover();
                // System.out.println("[SET " + set.setNumber + "] drained carryover count=" + carry.size());
                if (!carry.isEmpty()) {
                    // System.out.println("[SET " + set.setNumber + "] submitting carryover txns first: count=" + carry.size());
                    List<CompletableFuture<Void>> pres = new ArrayList<>(carry.size());
                    for (Transaction t : carry) pres.add(submitTxn(t, q));
                    waitAll(pres);
                    // System.out.println("[SET " + set.setNumber + "] carryover done");
                }
            }


            List<List<String>> regions = new ArrayList<>();
            regions.add(new ArrayList<>());
            for (String it : set.items) {
                if (it.equalsIgnoreCase("LF")) {

                    regions.add(List.of("LF"));
                    regions.add(new ArrayList<>());
                } else {
                    regions.get(regions.size()-1).add(it);
                }
            }

            if (!regions.isEmpty() && regions.get(regions.size()-1).isEmpty()) regions.remove(regions.size()-1);

            Random rnd = new Random();


            for (int i = 0; i < regions.size(); i++) {
                List<String> region = regions.get(i);
                if (region.size() == 1 && region.get(0).equalsIgnoreCase("LF")) {

                    int lid = LEADER.current();
                    String nodeId = (lid == -1) ? probeLeaderOrDefault("n1") : ("n" + lid);
                    // System.out.println("[SET " + set.setNumber + "] LF -> pausing " + nodeId);
                    LEADER.forget();
                    BLACKLIST.add(nodeId);
                    cmdSleep(nodeId, Long.MAX_VALUE / 4);

                    // System.out.println("[SET " + set.setNumber + "] Waiting for new leader election...");
                    Thread.sleep(ELECTION_GRACE_MS);
                    continue;
                }

                List<CompletableFuture<Void>> futures = new ArrayList<>();
                int nextClient = 0;
                for (String tok : region) {
                    String[] p = parseTriplet(tok);
                    String s = p[0], r = p[1]; int amt = Integer.parseInt(p[2]);
                    String cid = "C" + (1 + (nextClient++ % 10));   // C1..C10
                    Transaction t = buildTxn(cid, s, r, amt, rnd);
                    futures.add(submitTxn(t, q));
                }

                waitAll(futures);
                // System.out.println("[SET " + set.setNumber + "] region done (" + futures.size() + " txns)");
            }

            List<String> currentLive = set.liveNodes.stream()
                    .filter(id -> !BLACKLIST.contains(id))
                    .collect(java.util.stream.Collectors.toList());
            // System.out.println("[SET " + set.setNumber + "] Waiting for cluster convergence...");
            waitForConvergence(currentLive, 8000);
            try { Thread.sleep(300); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            pauseForPresentation(currentLive);
            System.out.println("=== END SET " + set.setNumber + " (timers paused for presentation) ===\n");
        }

        @Override public void run() {
            while (true) {
                try {
                    Transaction txn = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (txn == null) continue;
                    if (!INFLIGHT.containsKey(txn.getTimestamp())) {
                        // System.out.printf("[CLIENT %s] skipping stale txn (no inflight future): %s%n",
                        //         name, fmtTxn(txn));
                        enqueueCarryover(txn);
                        continue;
                    }
                    sendWithAttempts(name, txn, rnd, MIN_RETRY_ATTEMPTS_PER_TXN);
                } catch (InterruptedException ie) {
                    return;
                } catch (Throwable t) {
                    System.err.println("[CLIENT " + name + "] error: " + t);
                }
            }
        }

    }

    static String fmtTxn(Transaction t) {
        return "(" + t.getSender() + "->" + t.getReceiver() + ", " + t.getAmount() + ", ts=" + t.getTimestamp() + ")";
    }

    static String[] parseTriplet(String token) {
        String t = token.trim();
        t = t.replaceAll("[()]", "");
        String[] parts = t.split("\\s*,\\s*");
        if (parts.length != 3) throw new IllegalArgumentException("Bad triplet: " + token);
        return parts;
    }

static void applyLiveNodes(List<String> live) {
    Set<String> liveSet = new HashSet<>(live);
    List<CompletableFuture<Void>> tasks = new ArrayList<>();
    for (NodeInfo n : NODES.values()) {
        tasks.add(CompletableFuture.runAsync(() -> {
            try {
                var stub = n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS);
                if (liveSet.contains(n.id)) stub.resumeNodeRPC(ResumeRequest.newBuilder().build());
                else stub.pauseNodeRPC(PauseRequest.newBuilder().build());
            } catch (Exception e) {
                System.err.println("applyLiveNodes: " + n.id + " -> " + e.getMessage());
            }
        }));
    }
    for (CompletableFuture<Void> f : tasks) {
        try { f.get(); } catch (Exception ignored) {}
    }
}


    static void waitForConvergence(List<String> live, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int checkIntervalMs = 200;
        
        while (System.currentTimeMillis() < deadline) {
            try {

                Map<String, int[]> states = new HashMap<>();
                Map<String, Map<String, Integer>> balancesByNode = new HashMap<>();
                for (String id : live) {
                    NodeInfo n = NODES.get(id);
                    if (n == null) continue;
                    try {
                        DBDump db = n.stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                                .getDB(com.google.protobuf.Empty.getDefaultInstance());
                        states.put(id, new int[]{(int)db.getCommitIndex(), (int)db.getExecIndex()});
                        balancesByNode.put(id, new HashMap<>(db.getBalancesMap()));
                    } catch (Exception e) {

                        states.put(id, new int[]{-1, -1});
                        balancesByNode.put(id, Collections.emptyMap());
                    }
                }

                boolean allConverged = true;
                Integer commonCommit = null;
                for (var entry : states.entrySet()) {
                    int[] ce = entry.getValue();
                    if (ce[0] == -1) {
                        allConverged = false;
                        break;
                    }
                    if (ce[0] != ce[1]) {
                        allConverged = false;
                        break;
                    }
                    if (commonCommit == null) {
                        commonCommit = ce[0];
                    } else if (commonCommit != ce[0]) {
                        allConverged = false;
                        break;
                    }
                }

                boolean balancesEqual = false;
                if (allConverged && commonCommit != null) {
                    Map<String, Integer> ref = null;
                    balancesEqual = true;
                    for (String id : live) {
                        Map<String, Integer> b = balancesByNode.getOrDefault(id, Collections.emptyMap());
                        if (ref == null) {
                            ref = b;
                        } else if (!ref.equals(b)) {
                            balancesEqual = false;
                            break;
                        }
                    }
                }

                if (allConverged && balancesEqual && commonCommit != null) {
                    //System.out.println("[CONVERGENCE] All live nodes converged at commit=exec=" + commonCommit + " and balances match");
                    return;
                }
                
                Thread.sleep(checkIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        // System.err.println("CONVERGENCE Warning: Timeout waiting for convergence after " + timeoutMs + "ms");
    }

    static void pauseForPresentation(List<String> live) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String id : live) {
            NodeInfo n = NODES.get(id);
            if (n == null) continue;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                            .pauseNodeRPC(PauseRequest.newBuilder().build());
                } catch (Exception ignored) {}
            }));
        }
        for (CompletableFuture<Void> f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
    }

    static void resumeForNextSet(List<String> live) {
        for (String id : live) {
            NodeInfo n = NODES.get(id);
            if (n == null) continue;
            try {
                n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                        .resumeNodeRPC(ResumeRequest.newBuilder().build());
            } catch (Exception e) { /*Nothing*/ }
        }
    }


    static String probeLeaderOrDefault(String fallbackNodeId) {
        Ballot best = null;
        String bestId = null;
        for (NodeInfo n : NODES.values()) {
            if (BLACKLIST.contains(n.id)) continue;
            try {
                long ts = GLOBAL_TS_SEQ.incrementAndGet();
                Transaction tx = Transaction.newBuilder()
                        .setClientId("probe")
                        .setSender("A").setReceiver("A")
                        .setAmount(0).setTimestamp(ts).build();
                ClientReply rep = n.stub.withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                        .clientRequestRPC(ClientRequest.newBuilder().setTxn(tx).build());
                if (rep.hasReply() && rep.getReply().hasBallot()) {
                    Ballot b = rep.getReply().getBallot();
                    if (best == null || b.getRound() > best.getRound()) {
                        best = b;
                        bestId = "n" + b.getNodeId();
                    }
                }
            } catch (Exception ignore) {}
        }
        if (best != null) {
            LEADER.learnFromBallot(best);
            return bestId;
        }
        return fallbackNodeId;
    }


    static void warmUpCluster() {
        // System.out.println("Warming up client channels...");
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (NodeInfo n : NODES.values()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                            .ping(paxos.rpc.PingRequest.newBuilder().setFrom("clientWarmup").build());
                } catch (Exception ignored) {}
                try {
                    n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                            .getDB(Empty.getDefaultInstance());
                } catch (Exception ignored) {}
                try {
                    long ts = GLOBAL_TS_SEQ.incrementAndGet();
                    Transaction tx = Transaction.newBuilder()
                            .setClientId("warmup").setSender("A").setReceiver("A")
                            .setAmount(0).setTimestamp(ts).build();
                    n.stub.withDeadlineAfter(600, TimeUnit.MILLISECONDS)
                            .clientRequestRPC(ClientRequest.newBuilder().setTxn(tx).build());
                } catch (Exception ignored) {}
            }));
        }
        for (CompletableFuture<Void> f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
        // System.out.println("Warm-up complete.");
    }


    static final long RETRY_BUDGET_MS   = 5000;
    static final int  MIN_RETRY_ATTEMPTS_PER_TXN = 30; //  30 attempts
    static final long INITIAL_BACKOFF_MS= 75;
    static final long MAX_BACKOFF_MS    = 1000;
    static final double JITTER          = 0.20;

    static boolean isRetryable(String msg) {
        if (msg == null) return true;
        String m = msg.toUpperCase(Locale.ROOT);
        return m.contains("FORWARDING_ERROR") || m.contains("NO_LEADER")
                || m.contains("TIMEOUT") || m.contains("UNAVAILABLE")
                || m.contains("COULD NOT CONTACT LEADER")
                || m.contains("FAILED TO ACHIEVE A QUORUM")
                || m.contains("ELECTION_IN_PROGRESS")
                || m.contains("LEADER_TRANSITION")
                || m.contains("QUEUED");
    }

    static long jitter(long ms, Random r) {
        double f = 1.0 + (r.nextDouble()*2 - 1.0)*JITTER;
        long j = (long)Math.max(1.0, ms * f);
        return Math.min(j, MAX_BACKOFF_MS);
    }

    static List<NodeInfo> buildTargets(Random rnd) {
        int leader = LEADER.current();
        List<NodeInfo> t = new ArrayList<>();
        for (NodeInfo ni : NODES.values()) if (!BLACKLIST.contains(ni.id)) t.add(ni);
        if (t.isEmpty()) t = new ArrayList<>(NODES.values());
        if (leader != -1) {
            NodeInfo first = NODES.get("n" + leader);
            if (first != null) { t.remove(first); t.add(0, first); }
        } else {
            Collections.shuffle(t, rnd);
        }
        return t;
    }
    static void sendWithRetry(Transaction txn, Random rnd) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_BUDGET_MS);
        long backoff = INITIAL_BACKOFF_MS;
        while (System.nanoTime() < deadline) {
            for (NodeInfo n : buildTargets(rnd)) {
                try {
                    ClientRequest req = ClientRequest.newBuilder().setTxn(txn).build();
                    ClientReply rep = n.stub.withDeadlineAfter(600, TimeUnit.MILLISECONDS).clientRequestRPC(req);
                    if (!rep.hasReply()) continue;

                    TxnReply r = rep.getReply();
                    if (r.hasBallot()) LEADER.learnFromBallot(r.getBallot());


                    String sBal = r.getSuccess() ? String.valueOf(r.getSenderBalanceAfter()) : "-";
                    String rBal = r.getSuccess() ? String.valueOf(r.getReceiverBalanceAfter()) : "-";
                    System.out.printf("[CLIENT %s] %s -> success=%s msg=%s sender=%s receiver=%s%n",
                            txn.getClientId(), fmtTxn(txn), r.getSuccess(), r.getMessage(), sBal, rBal);


                    if (!r.getSuccess() && isRetryable(r.getMessage())) {
                        LEADER.forget();
                        break;
                    }


                    CompletableFuture<Void> f = INFLIGHT.remove(txn.getTimestamp());
                    if (f != null) f.complete(null);
                    return;
                } catch (Exception ignore) {

                }
            }
            TimeUnit.MILLISECONDS.sleep(jitter(backoff, rnd));
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
        }

        enqueueCarryover(txn);
        CompletableFuture<Void> f = INFLIGHT.remove(txn.getTimestamp());
        if (f != null) f.complete(null);
    }


    static void sendWithAttempts(String name, Transaction txn, Random rnd, int minAttempts) throws InterruptedException {
        int attempts = 0;
        long backoff = INITIAL_BACKOFF_MS;
        while (attempts < minAttempts) {
            for (NodeInfo n : buildTargets(rnd)) {
                attempts++;
                try {
                    // System.out.printf("[CLIENT %s][attempt %d/%d] target=%s sending %s%n",
                    //         name, attempts, minAttempts, n.id, fmtTxn(txn));
                    ClientRequest req = ClientRequest.newBuilder().setTxn(txn).build();
                    ClientReply rep = n.stub.withDeadlineAfter(600, TimeUnit.MILLISECONDS).clientRequestRPC(req);
                    if (!rep.hasReply()) {
                        // System.out.printf("[CLIENT %s][attempt %d/%d] target=%s no-reply%n",
                        //         name, attempts, minAttempts, n.id);
                        continue;
                    }

                    TxnReply r = rep.getReply();
                    if (r.hasBallot()) LEADER.learnFromBallot(r.getBallot());

                    // String sBal = r.getSuccess() ? String.valueOf(r.getSenderBalanceAfter()) : "-";
                    // String rBal = r.getSuccess() ? String.valueOf(r.getReceiverBalanceAfter()) : "-";
                    // System.out.printf("[CLIENT %s] %s -> success=%s msg=%s sender=%s receiver=%s%n",
                    //         name, fmtTxn(txn), r.getSuccess(), r.getMessage(), sBal, rBal);

                    if (!r.getSuccess() && isRetryable(r.getMessage())) {
                        // System.out.printf("[CLIENT %s][attempt %d/%d] retryable-failure; will retry (msg=%s)\n",
                        //         name, attempts, minAttempts, r.getMessage());
                        String _msg = (r.getMessage() == null) ? "" : r.getMessage();
                        String _upper = _msg.toUpperCase(java.util.Locale.ROOT);
                        if (_upper.contains("ELECTION_IN_PROGRESS") || _upper.contains("LEADER_TRANSITION")) {
                            // System.out.printf("[CLIENT %s][attempt %d/%d] election grace: sleeping %dms before retry%n",
                            //         name, attempts, minAttempts, ClientWorker.ELECTION_GRACE_MS);
                            java.util.concurrent.TimeUnit.MILLISECONDS.sleep(ClientWorker.ELECTION_GRACE_MS);
                        }
                        LEADER.forget();
                        continue;
                    }

                    // String outcome = r.getSuccess() ? "success" : "non-retryable";
                    // System.out.printf("[FINALIZE] ts=%d outcome=%s%n", txn.getTimestamp(), outcome);
                    CompletableFuture<Void> f = INFLIGHT.remove(txn.getTimestamp());
                    if (f != null) {
                        //System.out.printf("[COMPLETE] Completing future for ts=%d%n", txn.getTimestamp());
                        f.complete(null);
                    } else {
                        //System.out.printf("[WARNING] No future found for ts=%d (already completed?)%n", txn.getTimestamp());
                    }
                    return;
                } catch (Exception e) {
//                    System.out.printf("[CLIENT %s][attempt %d/%d] target=%s exception=%s%n",
//                            name, attempts, minAttempts, n.id, e.getMessage());
                }
            }
            TimeUnit.MILLISECONDS.sleep(jitter(backoff, rnd));
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
        }


        // System.out.printf("[ATTEMPTS_EXHAUSTED] ts=%d attempts=%d -> carryover%n", txn.getTimestamp(), attempts);
        // System.out.printf("[CLIENT %s] %s -> success=false msg=RetryAttemptsExceeded: tried %d attempts sender=- receiver=-\n",
        //         name, fmtTxn(txn), attempts);
        enqueueCarryover(txn);
        CompletableFuture<Void> f = INFLIGHT.remove(txn.getTimestamp());
        if (f != null) f.complete(null);
    }


    //  CSV loader → queues Used the Help of ChatGPT to write this
    static class TestSet {
        final int setNumber;
        final List<String> liveNodes;
        final List<String> items = new ArrayList<>();
        TestSet(int n, List<String> live) { setNumber = n; liveNodes = live; }
    }

    static List<TestSet> parseSetsCSV(String path) throws Exception {
        List<TestSet> sets = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Path.of(path))) {
            String header = br.readLine();
            String line;
            TestSet cur = null;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;

                List<String> cols = new ArrayList<>(3);
                StringBuilder sb = new StringBuilder();
                boolean inQ = false;
                for (int i = 0; i < line.length(); i++) {
                    char c = line.charAt(i);
                    if (c == '"') { inQ = !inQ; }
                    else if (c == ',' && !inQ) { cols.add(sb.toString().trim()); sb.setLength(0); }
                    else sb.append(c);
                }
                cols.add(sb.toString().trim());


                while (cols.size() < 3) cols.add("");

                String col0 = cols.get(0).trim();
                String col1 = cols.get(1).trim();
                String col2 = cols.get(2).trim();


                if (!col0.isEmpty()) {
                    int setNo = Integer.parseInt(col0);
                    List<String> live = new ArrayList<>();
                    if (!col2.isEmpty()) {

                        String s = col2.replaceAll("[\\[\\]\"]", "");
                        for (String tok : s.split("\\s*,\\s*")) {
                            if (!tok.isBlank()) live.add(tok.trim());
                        }
                    } else {

                        live = new ArrayList<>(List.of("n1","n2","n3","n4","n5"));
                    }
                    cur = new TestSet(setNo, live);
                    sets.add(cur);
                }

                if (cur == null) continue;


                if (!col1.isEmpty()) {
                    String token = col1.replaceAll("^\"|\"$", "").trim();
                    if (token.equalsIgnoreCase("LF")) cur.items.add("LF");
                    else cur.items.add(token);
                }
            }
        }
        return sets;
    }


    // ---- control helpers for REPL & CSV ---- Used the help of chatgpt to write this
    static void cmdPrintLog(String nodeId) {
        NodeInfo n = NODES.get(nodeId);
        if (n == null) { System.out.println("Unknown node: " + nodeId); return; }
        DBDump db = n.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).getDB(Empty.getDefaultInstance());
        LogDump dump = n.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).getLog(Empty.getDefaultInstance());
        System.out.println("=== LOG @ " + nodeId + " (commit=" + db.getCommitIndex() + ", exec=" + db.getExecIndex() + ") ===");
                for (LogRow r : dump.getRowsList()) {
            System.out.printf("idx=%-3d status=%s noop=%s txn=%s ballot=%s%n",
                    r.getIndex(), r.getStatus(), r.getIsNoop(), r.getTxn(),
                    r.hasBallot()? (r.getBallot().getRound()+"."+r.getBallot().getNodeId()) : "n/a");
        }
    }

    static void cmdPrintDB(String nodeId) {
        NodeInfo n = NODES.get(nodeId);
        if (n == null) { System.out.println("Unknown node: " + nodeId); return; }
        DBDump dump = n.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).getDB(Empty.getDefaultInstance());
        System.out.println("=== DB @ " + nodeId + " (commit=" + dump.getCommitIndex() + ", exec=" + dump.getExecIndex() + ") ===");
        dump.getBalancesMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.println(e.getKey() + "=" + e.getValue()));
    }

    static void cmdPrintStatus(long seq) {
        System.out.println("=== STATUS for seq " + seq + " ===");
        for (NodeInfo n : NODES.values()) {
            try {
                LogDump dump = n.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).getLog(Empty.getDefaultInstance());
                String st = "X";
                for (LogRow r : dump.getRowsList()) {
                    if (r.getIndex() == seq) { st = r.getStatus(); break; }
                }
                System.out.printf("%s: %s%n", n.id, st);
            } catch (Exception e) {
                System.out.printf("%s: ERROR (%s)%n", n.id, e.getMessage());
            }
        }
    }

    static void cmdPrintView() {
        System.out.println("=== NEW-VIEW history (per node) ===");
        for (NodeInfo n : NODES.values()) {
            try {
                ViewDump vd = n.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).getView(Empty.getDefaultInstance());
                System.out.println("[" + n.id + "] count=" + vd.getHistoryCount());
                int i = 0;
                for (ViewChange vc : vd.getHistoryList()) {
                    System.out.println("  #" + (++i) + " ballot=" + vc.getBallot().getRound() + "." + vc.getBallot().getNodeId()
                            + " entries=" + vc.getLogCount());
                }
            } catch (Exception e) {
                System.out.printf("%s: ERROR (%s)%n", n.id, e.getMessage());
            }
        }
    }
    static final ScheduledExecutorService GLOBAL_SCHEDULER =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "client-scheduler");
                t.setDaemon(true);
                return t;
            });

    static void cmdSleep(String nodeId, long ms) {
        NodeInfo n = NODES.get(nodeId);
        if (n == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                    .pauseNodeRPC(PauseRequest.newBuilder().build());
            System.out.println(nodeId + " paused for ~" + ms + "ms");
            Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                try { n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                        .resumeNodeRPC(ResumeRequest.newBuilder().build());
                    System.out.println(nodeId + " resumed");
                } catch (Exception ignored) {}
            }, ms, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.err.println("Pause failed for node " + nodeId + ": " + e.getMessage());
        }
    }


    static void shutdownAndExit() {
        NODES.values().forEach(node -> {
            try { node.close(); } catch (Exception ignored) {}
        });
        System.exit(0);
    }

    static void cmdWake(String nodeId) {
        NodeInfo n = NODES.get(nodeId);
        if (n == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            n.stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                    .resumeNodeRPC(ResumeRequest.newBuilder().build());
            System.out.println(nodeId + " resumed");
        } catch (Exception e) {
            System.err.println("Resume failed for node " + nodeId + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
        String csvPath = (args.length > 0) ? args[0] : Paths.get("CSE535-F25-Project-1-Testcases.csv").toString();


        NODES.put("n1", new NodeInfo("n1", "localhost", 60691));
        NODES.put("n2", new NodeInfo("n2", "localhost", 60692));
        NODES.put("n3", new NodeInfo("n3", "localhost", 60693));
        NODES.put("n4", new NodeInfo("n4", "localhost", 60694));
        NODES.put("n5", new NodeInfo("n5", "localhost", 60695));


        warmUpCluster();
          BlockingQueue<Transaction> q = new ArrayBlockingQueue<>(1000);
        for (int i = 0; i < 10; i++){ pool.submit(new ClientWorker("C"+(i+1), q));}

            List<TestSet> sets = parseSetsCSV(csvPath);
            System.out.println("Loaded " + sets.size() + " sets from " + csvPath);

            Executors.newSingleThreadExecutor().submit(ClientMain::repl);

            for (TestSet set : sets) {

                ClientWorker.runSet(set, q);
                // cmdPrintAllNodes();
                CountDownLatch latch = new CountDownLatch(1);


                STEP_LATCH.set(latch);
                System.out.println("Commands (client): printlog nX | printdb nX | printstatus <seq> | printview | sleep nX <ms> | wake nX | printall | next | exit");
                System.out.println("Commands (node nX): printdb | printlog | printview | printstatusall | printstatus <seq> | sleep | wake | exit");
                System.out.println("Paused. Type 'next' in client to continue...");
                try { latch.await(); } catch (InterruptedException ignored) {}
                STEP_LATCH.set(null);

                resumeForNextSet(set.liveNodes);
            }

            System.out.println("All sets processed. You can keep using the console below.");


            System.out.println("CSV instructions drained. You can keep using the console below.");
        } catch (Exception e) {
            System.err.println("Error in main: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        finally {
            try { pool.shutdownNow(); } catch (Exception ignored) {}
            try { GLOBAL_SCHEDULER.shutdownNow(); } catch (Exception ignored) {}
            NODES.values().forEach(n -> { try { n.close(); } catch (Exception ignored) {} });
        }
    }

    static final AtomicLong GLOBAL_TS_SEQ = new AtomicLong(System.currentTimeMillis() * 1_000_000L);
    
    static synchronized Transaction buildTxn(String clientId, String s, String r, int amt, Random rnd) {

        long ts = GLOBAL_TS_SEQ.incrementAndGet();
        return Transaction.newBuilder()
                .setClientId(clientId)
                .setSender(s).setReceiver(r).setAmount(amt).setTimestamp(ts).build();
    }



    static CompletableFuture<Void> submitTxn(Transaction t, BlockingQueue<Transaction> q) {
        // System.out.printf("[SUBMIT] Creating future for ts=%d clientId=%s %s%n",
        //         t.getTimestamp(), t.getClientId(), fmtTxn(t));
        CompletableFuture<Void> f = new CompletableFuture<>();
        INFLIGHT.put(t.getTimestamp(), f);
        try {
            q.put(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            INFLIGHT.remove(t.getTimestamp());
            f.completeExceptionally(e);
        }
        return f;
    }



    static void waitAll(List<CompletableFuture<Void>> futures) {
long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
for (CompletableFuture<Void> f : futures) {
    long left = deadline - System.nanoTime();
    if (left <= 0) break;
    try {
        f.get(left, TimeUnit.NANOSECONDS);
    } catch (Exception ignored) {}
}

// int done = 0;
// for (CompletableFuture<Void> f : futures) if (f.isDone()) done++;
// System.out.printf("[REGION] done=%d/%d%n", done, futures.size());

// futures.forEach(f -> {
//     if (!f.isDone()) {
//         System.out.println("[client] region timeout: a txn is still in-flight");
//     }
// });
}
    static Map<String, DBDump> fetchAllDBs() {
        Map<String, DBDump> out = new LinkedHashMap<>();
        for (Map.Entry<String, NodeInfo> e : NODES.entrySet()) {
            try {
                out.put(e.getKey(), e.getValue().stub
                        .withDeadlineAfter(2000, TimeUnit.MILLISECONDS)
                        .getDB(Empty.getDefaultInstance()));
            } catch (Exception ex) {
                // System.out.println("getDB " + e.getKey() + ": " + ex.getMessage());
            }
        }
        return out;
    }

    static Map<String, LogDump> fetchAllLogs() {
        Map<String, LogDump> out = new LinkedHashMap<>();
        for (Map.Entry<String, NodeInfo> e : NODES.entrySet()) {
            try {
                out.put(e.getKey(), e.getValue().stub
                        .withDeadlineAfter(2000, TimeUnit.MILLISECONDS)
                        .getLog(Empty.getDefaultInstance()));
            } catch (Exception ex) {
                // System.out.println("getLog " + e.getKey() + ": " + ex.getMessage());
            }
        }
        return out;
    }

    //Help taken from chatgpt for repl and CSV parser
    static void cmdPrintAllNodes() {
        System.out.println("\n========== CLUSTER SNAPSHOT ==========");
        Map<String, DBDump> dbs  = fetchAllDBs();
        Map<String, LogDump> logs = fetchAllLogs();

        for (String id : NODES.keySet()) {
            DBDump db = dbs.get(id);
            if (db == null) continue;
            System.out.println("\n=== DB @ " + id + " (commit=" + db.getCommitIndex() + ", exec=" + db.getExecIndex() + ") ===");
            db.getBalancesMap().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> System.out.println(e.getKey() + "=" + e.getValue()));
        }

        for (String id : NODES.keySet()) {
            LogDump dump = logs.get(id);
            if (dump == null) continue;
            System.out.println("\n=== LOG @ " + id + " ===");
            for (LogRow r : dump.getRowsList()) {
                System.out.printf("idx=%-3d status=%s noop=%s txn=%s ballot=%s%n",
                        r.getIndex(), r.getStatus(), r.getIsNoop(), r.getTxn(),
                        r.hasBallot()? (r.getBallot().getRound()+"."+r.getBallot().getNodeId()) : "n/a");
            }
        }

        printStatusMatrix(logs);


        cmdPrintView();
        System.out.println("======================================\n");
    }





    static void printStatusMatrix(Map<String, LogDump> logs) {
        SortedSet<Long> allIdx = new TreeSet<>();
        Map<String, Map<Long, String>> nodeToStatus = new LinkedHashMap<>();
        for (var e : logs.entrySet()) {
            Map<Long, String> m = new HashMap<>();
            LogDump d = e.getValue();
            if (d != null) {
                for (LogRow r : d.getRowsList()) {
                    allIdx.add(r.getIndex());
                    m.put(r.getIndex(), r.getStatus());
                }
            }
            nodeToStatus.put(e.getKey(), m);
        }

        System.out.println("\n=== STATUS MATRIX (A/C/E/X) ===");
        System.out.print(String.format("%-6s", "idx"));
        for (String id : NODES.keySet()) System.out.print(String.format(" | %-3s", id));
        System.out.println();
        System.out.println("-".repeat(8 + NODES.size() * 6));

        for (long idx : allIdx) {
            System.out.print(String.format("%-6d", idx));
            for (String id : NODES.keySet()) {
                String st = nodeToStatus.getOrDefault(id, Map.of()).getOrDefault(idx, "X");
                System.out.print(String.format(" | %-3s", st));
            }
            System.out.println();
        }
        System.out.println("================================\n");
    }



    // console for manual commands Help taken from chatgpt for repl and CSV parser
    static void repl() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Commands: printlog nX | printdb nX | printstatus <seq> | printview | sleep nX <ms> | wake nX | exit");
        while (true) {
            System.out.print("> ");
            String line = sc.nextLine();
            if (line == null) return;
            line = line.trim();
            if (line.equalsIgnoreCase("exit")) System.exit(0);
            if (line.isEmpty()) continue;
            String[] p = line.split("\\s+");
            try {
                switch (p[0].toLowerCase(Locale.ROOT)) {
                    case "printlog" -> cmdPrintLog(p[1]);
                    case "printdb" -> cmdPrintDB(p[1]);
                    case "printstatus" -> cmdPrintStatus(Long.parseLong(p[1]));
                    case "printview" -> cmdPrintView();
                    case "sleep" -> cmdSleep(p[1], Long.parseLong(p[2]));
                    case "wake" -> cmdWake(p[1]);
                    case "printall" -> cmdPrintAllNodes();
                    case "next" -> {
                        CountDownLatch l = STEP_LATCH.getAndSet(null);
                        if (l != null) {
                            l.countDown();
                        } else {
                            System.out.println("Nothing to continue; not currently paused.");
                        }
                    }
                    case "exit" -> shutdownAndExit();
                    default -> System.out.println("unknown");
                }
            } catch (Exception e) {
                System.out.println("error: " + e.getMessage());
            }
        }
    }
}
