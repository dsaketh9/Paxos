package paxos.Node;

import io.grpc.stub.StreamObserver;
import paxos.rpc.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node {

    private final String nodeId;
    private final Map<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peerStubs;
    private final Map<String, HostPort> directory;

    private final ExecutorService executor;
    public static final int ELECTION_TIMEOUT_MIN_MS = 500;
    public static final int ELECTION_TIMEOUT_MAX_MS = 800;
    public final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    public final Random random = new Random();
    public static final int PREPARE_RPC_DEADLINE_MS = 2000;
    public static final int VIEW_RPC_DEADLINE_MS    = 2000;
    public static final int COMMIT_RPC_DEADLINE_MS  = 1200;

    private volatile Ballot highestPromisedBallot;
    private volatile Ballot currentBallot;
    private volatile boolean isLeader = false;
    public volatile int leaderId = -1;
    public volatile int electionAttempts = 0;
    private final Map<Long, LogEntry> accepted = new ConcurrentHashMap<>();
    private final Map<Long, LogEntry> decided = new ConcurrentHashMap<>();
    private volatile long commitIndex = 0L;
    private volatile long nextLogIndex = 1L;
    private volatile long execIndex = 0L;
    private volatile boolean paused = false;
    private volatile long lastPauseAtMs = 0L;
    private volatile long lastHbLogMs = 0;
    private ScheduledFuture<?> electionTask;

    public ScheduledFuture<?> electionTimerTask;
    private ScheduledFuture<?> heartbeatTask;
    public static final int HEARTBEAT_INTERVAL_MS = 150;
    public final AtomicBoolean electionTimerExpired = new AtomicBoolean(false);
    private Runnable electionTimeoutLogic;
    private static final long PREPARE_SUPPRESSION_MS = initPrepareSuppressionMs();
    private final List<PendingPrepare> pendingPrepareObservers = new CopyOnWriteArrayList<>();
    private volatile long lastPrepareAtMs = 0L;
    private final List<ViewChange> viewChangeHistory = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, Long> lastReplyTs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TxnReply> lastReply = new ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentLinkedQueue<Transaction> pendingClientQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();


    private final java.util.concurrent.ConcurrentMap<String, Long> executedRequestIndex = new java.util.concurrent.ConcurrentHashMap<>();
    public Node(String nodeId,
                Map<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peerStubs,
                Map<String, PaxosServer.Addr> directoryIn) {
        this.nodeId = nodeId;
        this.peerStubs = peerStubs;
        this.directory = new java.util.HashMap<>();
        for (var e : directoryIn.entrySet()) {
            this.directory.put(e.getKey(), new HostPort(e.getValue().host, e.getValue().port));
        }
        this.executor = Executors.newFixedThreadPool(this.peerStubs.size());
        this.snapshotPath = Paths.get("data", nodeId + "-kv.snapshot");

        // CSV flags.
        this.csvExportEnabled = "1".equals(System.getenv("PAXOS_CSV_EXPORT"))
                || Boolean.parseBoolean(System.getProperty("paxos.csv.export", "false"));
        this.csvImportEnabled = "1".equals(System.getenv("PAXOS_CSV_IMPORT"))
                || Boolean.parseBoolean(System.getProperty("paxos.csv.import", "false"));



        List.of("A", "B", "C", "D", "E", "F", "G", "H", "I", "J").forEach(client -> balance.put(client, 10));

        this.isLeader = false;
        this.leaderId = -1;

        Ballot zeroBallot = Ballot.newBuilder().setRound(0).setNodeId(0).build();
        this.highestPromisedBallot = zeroBallot;
        this.currentBallot = zeroBallot;

        //Gradle sometimes takes old snapshot builds so to solve that issue we are refreshing the snapshot
        if (!Boolean.parseBoolean(System.getProperty("paxos.fresh", "false"))
                && !"1".equals(System.getenv("PAXOS_FRESH"))) {
            loadSnapshotIfPresent();
        }

        this.electionTimeoutLogic = () -> {
            try {
                if (onElectionTimeout()) startElection();
            } catch (Throwable t) {
                // System.err.println("Node " + nodeId + ": electionTimeoutLogic crashed: " + t);
            }
        };
        resetElectionTimer();

    }


    static final class HostPort {
        final String host; final int port;
        HostPort(String h, int p) { host = h; port = p; }
    }

    private static long initPrepareSuppressionMs() {
        try {
            String v = System.getenv("PAXOS_PREPARE_SUPPRESS_MS");
            if (v != null && !v.isBlank()) {
                long parsed = Long.parseLong(v.trim());
                if (parsed >= 50 && parsed <= 2000) return parsed;
            }
        } catch (Exception ignored) {}
        return 200L;
    }
    private static final class PendingPrepare {
        final Prepare request;
        final StreamObserver<PrepareResult> observer;
        final long receivedAt;

        PendingPrepare(Prepare request, StreamObserver<PrepareResult> observer, long receivedAt) {
            this.request = request;
            this.observer = observer;
            this.receivedAt = receivedAt;
        }
    }

    private final Map<String, Integer> balance = new ConcurrentHashMap<>();
    private final Path snapshotPath;
    private final boolean csvExportEnabled;
    private final boolean csvImportEnabled;
    private boolean isStale(String clientId, long ts) {
        return lastReplyTs.getOrDefault(clientId, Long.MIN_VALUE) > ts;
    }
    private TxnReply replayIfSameTs(String clientId, long ts) {
        return (lastReplyTs.getOrDefault(clientId, Long.MIN_VALUE) == ts) ? lastReply.get(clientId) : null;
    }
    private void rememberReply(String clientId, long ts, TxnReply r) {
        lastReplyTs.put(clientId, ts);
        lastReply.put(clientId, r);
    }
    private static String reqKey(String clientId, long ts) { return clientId + "#" + ts; }
    public synchronized void pauseNode() {
        if (!paused) {
            paused = true;
            lastPauseAtMs = System.currentTimeMillis();
            if (electionTimerTask != null && !electionTimerTask.isDone()) electionTimerTask.cancel(false);
            stopHeartbeat();
            System.out.println("Node " + nodeId + " paused (disconnected).");
        }
    }

    public synchronized void resumeNode() {
        if (!paused) return;
        paused = false;
        resetElectionTimer();
        if (isLeader) startHeartbeat();
        else {
            executor.submit(this::syncFromCluster);
        }
        // System.out.println("Node " + nodeId + ": RESUMED");
    }

    public boolean isPaused() { return paused; }

    public synchronized void pauseAllTimersForPresentation() {
        if (electionTimerTask != null && !electionTimerTask.isDone()) {
            electionTimerTask.cancel(false);
        }
        stopHeartbeat();
    }

    public void startHeartbeat() {
        stopHeartbeat();
        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            if (isLeader && !paused) {
                sendHeartbeats();
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        // System.out.println("Node " + nodeId + ": Started heartbeat task");
    }
    
    public void stopHeartbeat() {
        if (heartbeatTask != null && !heartbeatTask.isDone()) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    private void sendHeartbeats() {
        if (!isLeader || paused) return;
        Commit hb = Commit.newBuilder()
                .setBallot(this.currentBallot)
                .setIndex(this.commitIndex)
                .setIsNoop(true)
                .build();
        for (var peer : peerStubs.entrySet()) {
            PaxosServiceGrpc.PaxosServiceStub async = PaxosServiceGrpc.newStub(peer.getValue().getChannel());
            async.commitRPC(hb, new StreamObserver<CommitAck>() {
                @Override public void onNext(CommitAck value) {}
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            });
            // Suppress heartbeat broadcast logs
            // System.out.println("Node " + nodeId + " (Leader): HB -> all, ballot " + formatBallot(this.currentBallot)
            //         + ", commit=" + this.commitIndex);

        }
    }

    public synchronized PaxosServiceGrpc.PaxosServiceBlockingStub getOrCreateStub(String id) {
        PaxosServiceGrpc.PaxosServiceBlockingStub s = this.peerStubs.get(id);
        if (s != null) return s;
        HostPort hp = this.directory.get(id);
        if (hp == null) return null;
        var ch = io.grpc.ManagedChannelBuilder.forAddress(hp.host, hp.port).usePlaintext().build();
        s = PaxosServiceGrpc.newBlockingStub(ch);
        this.peerStubs.put(id, s);
        return s;
    }

    public synchronized void markLeaderUnknownAndTriggerElection() {
        this.isLeader = false;
        this.leaderId = -1;
        resetElectionTimer();
        executor.submit(this::startElection);
    }



    public ClientReply handleClientRequest(Transaction txn) {
        if (txn.getAmount() <= 0 || txn.getSender().equals(txn.getReceiver())) {
            TxnReply bad = TxnReply.newBuilder()
                    .setTimestamp(txn.getTimestamp())
                    .setBallot(this.currentBallot)
                    .setSuccess(false)
                    .setMessage(txn.getAmount() <= 0
                            ? "Invalid amount (must be > 0)"
                            : "Invalid transfer (sender == receiver)")
                    .build();
            rememberReply(txn.getClientId(), txn.getTimestamp(), bad);
            return ClientReply.newBuilder().setReply(bad).build();
        }

        String cid = txn.getClientId();
        long ts = txn.getTimestamp();
        if (isStale(cid, ts)) {
            TxnReply r = lastReply.get(cid);
            return ClientReply.newBuilder().setReply(
                    r != null ? r : TxnReply.newBuilder()
                            .setTimestamp(ts).setSuccess(false).setMessage("Stale request").setBallot(this.currentBallot).build()
            ).build();
        }
        TxnReply cached = replayIfSameTs(cid, ts);
        if (cached != null) {
            //System.out.printf("Node %s:  CACHE HIT for (%s, %d) - returning cached reply WITHOUT new log entry%n",
                    //nodeId, cid, ts);
            return ClientReply.newBuilder().setReply(cached).build();
        }

        long index;
        synchronized (this) {
            if (this.nextLogIndex <= this.commitIndex) {
                this.nextLogIndex = this.commitIndex + 1;
            }
            index = this.nextLogIndex++;
             }

        LogEntry entry = LogEntry.newBuilder()
                .setBallot(this.currentBallot)
                .setIndex(index)
                .setTxn(txn)
                .setIsNoop(false)
                .build();

        //System.out.println("Node " + nodeId + " (Leader): Proposing entry for index " + index);

        boolean quorumReached = runAcceptPhase(entry);

        if (quorumReached) {
            //System.out.println("Node " + nodeId + " (Leader): Quorum reached for index " + index + ". Committing.");
            commitEntry(entry);
            waitUntilExecuted(index, 1500);
            TxnReply done = lastReply.get(cid);
            if (done == null) {

                done = TxnReply.newBuilder().setTimestamp(ts).setBallot(this.currentBallot)
                        .setSuccess(true).setMessage("Committed").build();
            }
            return ClientReply.newBuilder().setReply(done).build();
        } else {
            //System.out.println("Node " + nodeId + " (Leader): Quorum failed for index " + index + ". Txn rejected.");
            TxnReply reply = TxnReply.newBuilder()
                    .setTimestamp(ts)
                    .setSuccess(false)
                    .setMessage("Transaction failed to achieve a quorum.")
                    .setBallot(this.currentBallot)
                    .build();

            return ClientReply.newBuilder().setReply(reply).build();
        }
    }

    private boolean runAcceptPhase(LogEntry entry) {
        if (entry.getIndex() <= this.commitIndex) {
//            System.err.println("Node " + nodeId + ": Refusing to accept index " + entry.getIndex()
//                    + " <= commitIndex " + this.commitIndex);
            return false;
        }
        addToAcceptedLog(entry);

        Accept acceptRequest = Accept.newBuilder()
                .setBallot(entry.getBallot())
                .setIndex(entry.getIndex())
                .setTxn(entry.getTxn())
                .setIsNoop(entry.getIsNoop())
                .build();

        int acceptanceCount = 1;
        int quorumSize = getQuorumSize();

        for (Map.Entry<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peer : peerStubs.entrySet()) {
            try {
                AcceptResult result = peer.getValue()
                        .withDeadlineAfter(PREPARE_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS)
                        .acceptRPC(acceptRequest);
                if (result != null) {
                    if (result.hasAccepted() && result.getAccepted().getOk()) {
                        acceptanceCount++;
                    } else if (result.hasReject()) {
                        updateHighestPromisedBallot(result.getReject().getPromisedBallot());
                        synchronized (this) {
                            if (compareBallots(this.highestPromisedBallot, this.currentBallot) > 0) {
                                this.isLeader = false;
                                this.leaderId = -1;
                                stopHeartbeat();
                            }
                        }
                        resetElectionTimer();
                        return false;
                    }
                }
            } catch (Exception e) {
                System.err.println("Node " + nodeId + " (Leader): Error contacting " + peer.getKey() + " for accept.");
            }
        }

        return acceptanceCount >= quorumSize;
    }

    private void commitEntry(LogEntry entry) {
        this.decided.put(entry.getIndex(), entry);
        //this.accepted.remove(entry.getIndex());
        this.commitIndex = Math.max(commitIndex, entry.getIndex());
        broadcastCommit(entry);
        executeStateMachine();
    }

    private void broadcastCommit(LogEntry entry) {
        System.out.println("Node " + nodeId + ": Broadcasting commit for index " + entry.getIndex());
        Commit commitRequest = Commit.newBuilder()
                .setBallot(entry.getBallot())
                .setIndex(entry.getIndex())
                .setTxn(entry.getTxn())
                .setIsNoop(entry.getIsNoop())
                .build();

        for (Map.Entry<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peer : peerStubs.entrySet()) {
            final String peerId = peer.getKey();
            final PaxosServiceGrpc.PaxosServiceBlockingStub stub = peer.getValue();
            executor.submit(() -> {
                try {
                    PaxosServiceGrpc.PaxosServiceBlockingStub deadlineStub =
                            stub.withDeadlineAfter(COMMIT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    deadlineStub.commitRPC(commitRequest);

                } catch (Exception e) {
                    System.err.println("Node " + nodeId + ": Could not send commit to " + peerId + ". " + e.getMessage());
                }
            });
        }
    }

    public synchronized void handleCommit(Commit request) {
        if (paused) return;
        

        updateHighestPromisedBallot(request.getBallot());
        if (compareBallots(request.getBallot(), this.currentBallot) > 0) {
            this.currentBallot = request.getBallot();
        }
        int prevLeader = this.leaderId;
        this.leaderId = request.getBallot().getNodeId();
        if (prevLeader != this.leaderId) {
            System.out.println("[LEADER] n" + this.leaderId);
        }

        if (request.getIsNoop()) {

                long now = System.currentTimeMillis();
                if (now - lastHbLogMs > 9500) {
                    // Suppress follower heartbeat logs
                    // System.out.println("Node " + nodeId + ": HB from leader n" + request.getBallot().getNodeId()
                    //         + " b=" + formatBallot(request.getBallot()) + " commitAdv=" + request.getIndex());
                    lastHbLogMs = now;
                }


            this.commitIndex = Math.max(this.commitIndex, request.getIndex());
            executeStateMachine();
            resetElectionTimer();
            boolean needSync = (this.decided.get(this.commitIndex) == null);
            if (!needSync) {
                long start = this.execIndex + 1;
                long end = this.commitIndex;
                for (long i = start; i <= end; i++) {
                    if (!this.decided.containsKey(i)) { needSync = true; break; }
                }
            }
            if (needSync) executor.submit(this::syncFromCluster);
            return;
        }

        LogEntry entry = LogEntry.newBuilder()
                .setBallot(request.getBallot())
                .setIndex(request.getIndex())
                .setTxn(request.getTxn())
                .setIsNoop(request.getIsNoop())
                .build();

        this.decided.put(entry.getIndex(), entry);
        //this.accepted.remove(entry.getIndex());
        this.commitIndex = Math.max(commitIndex, entry.getIndex());
        System.out.println("Node " + nodeId + ": Received commit for index " + entry.getIndex());
        executeStateMachine();
        resetElectionTimer();
        boolean needSync = false;
        long start = this.execIndex + 1;
        long end = this.commitIndex;
        for (long i = start; i <= end; i++) {
            if (!this.decided.containsKey(i)) { needSync = true; break; }
        }
        if (needSync) executor.submit(this::syncFromCluster);
    }

    private final ConcurrentHashMap<Long, CompletableFuture<Void>> executed = new ConcurrentHashMap<>();

    private void signalExecuted(long idx) {
        executed.computeIfAbsent(idx, k -> new CompletableFuture<>()).complete(null);
    }

    private void waitUntilExecuted(long idx, long timeoutMs) {
        try {
            executed.computeIfAbsent(idx, k -> new CompletableFuture<>()).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception ignore) {}
    }


    private synchronized void executeStateMachine() {
        boolean snapshotNeeded = false;
        while (execIndex + 1 <= commitIndex) {
            long next = execIndex + 1;
            LogEntry entryToExecute = decided.get(next);
            if (entryToExecute == null) break;

            if (!entryToExecute.getIsNoop()) {
                Transaction txn = entryToExecute.getTxn();
                String cid = txn.getClientId();
                long ts = txn.getTimestamp();
                String key = reqKey(cid, ts);
                Long prev = executedRequestIndex.putIfAbsent(key, next);
                if (prev == null) {
                    int amt = txn.getAmount();
                    int fromBal = balance.getOrDefault(txn.getSender(), 0);
                    int toBal   = balance.getOrDefault(txn.getReceiver(), 0);

                    boolean ok = fromBal >= amt;
                    String msg = ok ? "Transaction successful." : "Insufficient funds.";
                    if (ok) {
                        fromBal -= amt;
                        toBal   += amt;
                        balance.put(txn.getSender(), fromBal);
                        balance.put(txn.getReceiver(), toBal);
                    }

                    TxnReply reply = TxnReply.newBuilder()
                            .setBallot(this.currentBallot)
                            .setTimestamp(ts)
                            .setSuccess(ok)
                            .setMessage(msg)
                            .setSenderBalanceAfter(fromBal)
                            .setReceiverBalanceAfter(toBal)
                            .build();

                    rememberReply(cid, ts, reply);
                    snapshotNeeded = true;
                } else {
                    //System.out.println("Node " + nodeId + ": [DEDUP] skip duplicate (" + cid + "," + ts + ") at idx " + next + " firstSeen=" + prev);
                }
            }
            execIndex = next;
            signalExecuted(execIndex);
            snapshotNeeded = true;
        }
        if (snapshotNeeded) {
            saveSnapshot();
        }
    }


    public synchronized void startElection() {
        if (paused) return;
        long now = System.currentTimeMillis();
        long backoffMs = PREPARE_SUPPRESSION_MS * (1L << Math.min(electionAttempts, 5));
        if (now - lastPrepareAtMs < backoffMs) {
            System.out.println("Node " + nodeId + ": Backing off election for " + backoffMs + "ms");
            resetElectionTimer();
            return;
        }

        electionAttempts++;
        if (now - lastPrepareAtMs < PREPARE_SUPPRESSION_MS) {
            System.out.println("Node " + nodeId + ": Skipping election due to recent prepare message.");
            resetElectionTimer();
            return;
        }

        long newRound = this.currentBallot.getRound() + 1;
        int nodeIdNum = Integer.parseInt(nodeId.substring(1));

        this.currentBallot = Ballot.newBuilder().setRound(newRound).setNodeId(nodeIdNum).build();
        updateHighestPromisedBallot(this.currentBallot);
        lastPrepareAtMs = now;
        System.out.println("Node " + nodeId + ": Starting election with ballot " + formatBallot(this.currentBallot));

        Prepare prepareRequest = Prepare.newBuilder().setBallot(this.currentBallot).build();
        List<Future<PrepareResult>> promiseFutures = new ArrayList<>();
//        System.out.println("Node " + nodeId + ": peerStubs contains: " + peerStubs.keySet());
        for (Map.Entry<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peer : peerStubs.entrySet()) {
            final String peerId = peer.getKey();
//            System.out.println("Node " + nodeId + ": Sending PREPARE to " + peerId);
            Future<PrepareResult> future = executor.submit(() -> {
                try {
                    PaxosServiceGrpc.PaxosServiceBlockingStub deadlineStub =
                            peer.getValue().withDeadlineAfter(PREPARE_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    PrepareResult result = deadlineStub.prepareRPC(prepareRequest);

                    if (result != null) {
                        if (result.hasPromise()) {
//                            System.out.println("Node " + nodeId + ": Got PROMISE from " + peerId);
                        } else if (result.hasReject()) {
//                            System.out.println("Node " + nodeId + ": Got REJECT from " + peerId + " reason: " + result.getReject().getReason());
                        }
                    } else {
//                        System.out.println("Node " + nodeId + ": Got null result from " + peerId);
                    }

                    return result;
                } catch (Exception e) {
                    System.err.println("Node " + nodeId + ": Could not contact " + peerId + " for Prepare RPC.");
                    return null;
                }
            });
            promiseFutures.add(future);
        }

        int promiseCount = 1;
        List<Promise> successfulPromises = new ArrayList<>();
        for (Future<PrepareResult> future : promiseFutures) {
            try {
                PrepareResult result = future.get(PREPARE_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                if (result != null && result.hasPromise() && result.getPromise().getOk()) {
                    promiseCount++;
                    successfulPromises.add(result.getPromise());
                }
            } catch (Exception ignored) { /* timeout/failure; don't count */ }
        }

        if (promiseCount < getQuorumSize()) {
            System.out.println("Node " + nodeId + ": Election lost with " + promiseCount + " promises.");
            stopHeartbeat();
            return;
        }

        System.out.println("Node " + nodeId + ": Election WON with " + promiseCount + " promises. Becoming leader.");
        this.isLeader = true;
        this.leaderId = nodeIdNum;
        this.highestPromisedBallot = this.currentBallot;


        startHeartbeat();
        Map<Long, LogEntry> consolidatedLog = new HashMap<>();
        consolidatedLog.putAll(this.accepted);
        for (Promise p : successfulPromises) {
            for (LogEntry entry : p.getLogList()) {
                long idx = entry.getIndex();
                if (!consolidatedLog.containsKey(idx) ||
                        compareBallots(entry.getBallot(), consolidatedLog.get(idx).getBallot()) > 0) {
                    consolidatedLog.put(idx, entry);
                }
            }
        }

        System.out.println("Node " + nodeId + ": Consolidating log and filling gaps...");
        long maxIndex = 0;
        if (!consolidatedLog.isEmpty()) {
            maxIndex = Collections.max(consolidatedLog.keySet());
        }

        for (long i = 1; i <= maxIndex; i++) {
            if (!consolidatedLog.containsKey(i)) {
                if (decided.containsKey(i)) {
                    consolidatedLog.put(i, decided.get(i));
                    System.out.println("Node " + nodeId + ": Adding committed entry at index " + i + " to NEW-VIEW.");
                } else {
                    System.out.println("Node " + nodeId + ": Found gap at index " + i + ". Inserting no-op.");
                    LogEntry noOp = LogEntry.newBuilder()
                            .setBallot(this.currentBallot)
                            .setIndex(i)
                            .setIsNoop(true)
                            .build();
                    consolidatedLog.put(i, noOp);
                }
            }
        }


        List<Long> orderedIndices = new ArrayList<>(consolidatedLog.keySet());
        Collections.sort(orderedIndices);
        List<LogEntry> orderedEntries = new ArrayList<>(orderedIndices.size());
        for (Long idx : orderedIndices) {
            LogEntry base = consolidatedLog.get(idx);
            LogEntry out;
            if (this.decided.containsKey(idx)) {
                out = LogEntry.newBuilder(this.decided.get(idx)).build();
            } else {
                out = LogEntry.newBuilder(base)
                        .setBallot(this.currentBallot)
                        .setIndex(idx)
                        .build();
            }
            consolidatedLog.put(idx, out);
            orderedEntries.add(out);
        }

        Set<String> seenReqs = new HashSet<>();
        List<LogEntry> dedupedEntries = new ArrayList<>(orderedEntries.size());
        for (LogEntry e : orderedEntries) {
            if (e.getIsNoop()) {
                dedupedEntries.add(e);
                continue;
            }
            Transaction t = e.getTxn();
            String key = t.getClientId() + "#" + t.getTimestamp();
            if (seenReqs.add(key)) {
                dedupedEntries.add(e);
            } else {
                LogEntry noop = LogEntry.newBuilder(e)
                        .setIsNoop(true)
                        .build();
                consolidatedLog.put(e.getIndex(), noop);
                dedupedEntries.add(noop);

            }
        }
        orderedEntries = dedupedEntries;

//        System.out.println("Node " + nodeId + ": Broadcasting NEW-VIEW and awaiting quorum of ViewAck to establish leadership.");

        ViewChange viewChangeRequest = ViewChange.newBuilder()
                .setBallot(this.currentBallot)
                .addAllLog(orderedEntries)
                .build();

        this.viewChangeHistory.add(viewChangeRequest);


        List<Future<ViewChangeAck>> ackFutures = new ArrayList<>();
        for (Map.Entry<String, PaxosServiceGrpc.PaxosServiceBlockingStub> peer : peerStubs.entrySet()) {
            final String peerId = peer.getKey();
            final PaxosServiceGrpc.PaxosServiceBlockingStub stub = peer.getValue();
            Future<ViewChangeAck> fut = executor.submit(() -> {
                try {
                    PaxosServiceGrpc.PaxosServiceBlockingStub deadlineStub =
                            stub.withDeadlineAfter(VIEW_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    return deadlineStub.viewChangeRPC(viewChangeRequest);
                } catch (Exception e) {
                    System.err.println("Node " + nodeId + ": NEW-VIEW to " + peerId + " failed: " + e.getMessage());
                    return null;
                }
            });
            ackFutures.add(fut);
        }


        this.accepted.clear();
        this.accepted.putAll(consolidatedLog);

        int quorum = getQuorumSize();
        int peersOk = 1;
        Map<Long, Integer> indexToAckCount = new HashMap<>();
        for (LogEntry e : orderedEntries) indexToAckCount.put(e.getIndex(), 1);

        for (Future<ViewChangeAck> f : ackFutures) {
            try {
                ViewChangeAck ack = f.get(VIEW_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                if (ack != null && ack.getOk()) {
                    peersOk++;
                    for (long idx : ack.getAcceptedIndicesList()) {
                        indexToAckCount.merge(idx, 1, Integer::sum);
                    }
                }
            } catch (Exception ignored) { /* timeout/failure */ }
        }

        if (orderedEntries.isEmpty()) {
            if (peersOk >= quorum) {
                this.nextLogIndex = Math.max(this.nextLogIndex, this.commitIndex + 1);
                System.out.println("Node " + nodeId + ": NEW-VIEW empty but quorum of acks (" + peersOk + "/" + quorum + "). Leadership established.");
                startHeartbeat();
                drainPendingAsLeader();
                return;
            } else {
                System.err.println("Node " + nodeId + ": NEW-VIEW empty but insufficient acks (" + peersOk + "/" + quorum + "); stepping down.");
                synchronized (this){this.isLeader = false; this.leaderId = -1; stopHeartbeat();}
                resetElectionTimer();
                return;
            }
        }

        boolean anyIndexQuorum = false;
        for (LogEntry e : orderedEntries) {
            if (indexToAckCount.getOrDefault(e.getIndex(), 0) >= quorum) { anyIndexQuorum = true; break; }
        }
        if (!anyIndexQuorum) {
            System.err.println("Node " + nodeId + ": NEW-VIEW had no index with quorum; stepping down.");
            this.isLeader = false; this.leaderId = -1; stopHeartbeat();
            resetElectionTimer();
            return;
        }

        this.nextLogIndex = Math.max(this.nextLogIndex, Math.max(this.commitIndex, maxIndex) + 1);

        for (LogEntry entry : orderedEntries) {
            int cnt = indexToAckCount.getOrDefault(entry.getIndex(), 0);
            if (cnt >= quorum) {
                commitEntry(entry);
            } else {
                System.out.println("Node " + nodeId + ": NEW-VIEW index " + entry.getIndex() +
                        " has only " + cnt + " acks (< quorum). Leaving as ACCEPTED.");
            }
        }

        System.out.println("Node " + nodeId + ": Leadership established. Ready to accept new requests.");
        drainPendingAsLeader();
    }
    public void maybeStartElectionOnClientRequest() {
        if (paused) {
            return;
        }
        long now = System.currentTimeMillis();
        if (!isLeader && getLeaderId() == -1 && now - lastPrepareAtMs >= PREPARE_SUPPRESSION_MS) {
            executor.submit(this::startElection);
        }
    }

    public void enqueueClientRequest(Transaction txn) {
        pendingClientQueue.offer(txn);
    }

    private void drainPendingAsLeader() {
        Transaction txn;
        while ((txn = pendingClientQueue.poll()) != null) {
            try {
                handleClientRequest(txn);
            } catch (Exception e) {
                System.err.println("Node " + nodeId + ": Failed to replay queued txn " + txn.getClientId() + " - " + e.getMessage());
            }
        }
    }

    public synchronized boolean onElectionTimeout() {
        this.electionTimerExpired.set(true);
        System.out.println("Node " + nodeId + ":  Election timer expired.");

        PendingPrepare best = null;
        List<PendingPrepare> others = new ArrayList<>();
        for (PendingPrepare pending : pendingPrepareObservers) {
            if (best == null) {
                best = pending;
                continue;
            }

            int ballotCompare = compareBallots(pending.request.getBallot(), best.request.getBallot());
            if (ballotCompare > 0 || (ballotCompare == 0 && pending.receivedAt < best.receivedAt)) {
                if (best != null) {
                    others.add(best);
                }
                best = pending;
            } else {
                others.add(pending);
            }
        }
        pendingPrepareObservers.clear();

        if (best != null) {
            System.out.println("Node " + nodeId + ": Granting pending prepare for ballot " +
                    formatBallot(best.request.getBallot()));
            sendPromise(best);
            for (PendingPrepare other : others) {
                sendReject(other, "HIGHER_BALLOT_CHOSEN");
            }
            resetElectionTimer();
            this.electionTimerExpired.set(false);
            return false;
        }

        long now = System.currentTimeMillis();
        if (now - lastPrepareAtMs < PREPARE_SUPPRESSION_MS) {
            System.out.println("Node " + nodeId + ": Recent prepare observed; deferring self-election.");
            resetElectionTimer();
            this.electionTimerExpired.set(false);
            return false;
        }

        return true;
    }

    public void resetElectionTimer() {
        if (electionTimerTask != null && !electionTimerTask.isDone()) {
            electionTimerTask.cancel(false);
        }
        this.electionTimerExpired.set(false);
        long randomizedTimeout = ELECTION_TIMEOUT_MIN_MS + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        if (this.electionTimeoutLogic != null) {
            electionTimerTask = scheduler.schedule(this.electionTimeoutLogic, randomizedTimeout, TimeUnit.MILLISECONDS);
        } else {
            System.err.println("Warning: Election timeout logic not set. Timer will not be rescheduled.");
        }
    }

    public synchronized String getNodeId() { return this.nodeId; }
    public synchronized boolean isLeader() { return this.isLeader; }
    public synchronized int getLeaderId() { return this.leaderId; }
    public synchronized long getCommitIndex() { return this.commitIndex; }
    public boolean hasElectionTimerExpired() { return this.electionTimerExpired.get(); }
    private int getQuorumSize() {
        int totalNodes = 5;
        return (totalNodes / 2) + 1;
//        int totalNodes = this.peerStubs.size() + 1;
//        return (totalNodes / 2) + 1;
      }

    public void setElectionTimeoutTask(Runnable task) { this.electionTimeoutLogic = task; }
    public synchronized Ballot getHighestPromisedBallot() { return this.highestPromisedBallot; }
    public synchronized Map<Long, LogEntry> getAcceptedLog() { return new HashMap<>(this.accepted); }
    public synchronized Map<Long, LogEntry> getDecidedLog() { return new HashMap<>(this.decided); }
    public synchronized void addToAcceptedLog(LogEntry entry) { this.accepted.put(entry.getIndex(), entry); }


    public Map<String, PaxosServiceGrpc.PaxosServiceBlockingStub> getPeerStubs() {
        return this.peerStubs;
    }

    public synchronized void updateHighestPromisedBallot(Ballot newBallot) {
        if (compareBallots(newBallot, this.highestPromisedBallot) > 0) {
            this.highestPromisedBallot = newBallot;
        }
    }


public void handlePrepare(Prepare request, StreamObserver<PrepareResult> observer) {
    if (paused) {
        sendReject(new PendingPrepare(request, observer, System.currentTimeMillis()), "NODE_PAUSED");
        return;
    }

    final long now = System.currentTimeMillis();
    final Ballot incoming = request.getBallot();

    boolean reject;
    synchronized (this) {
        lastPrepareAtMs = now;
        reject = (compareBallots(incoming, this.highestPromisedBallot) < 0);
        if (!reject) {

            this.highestPromisedBallot = incoming;
            if (compareBallots(incoming, this.currentBallot) > 0) {
                this.currentBallot = incoming;
            }

            if (this.isLeader && compareBallots(incoming, this.currentBallot) >= 0) {
                this.isLeader = false;
                this.leaderId = -1;
                stopHeartbeat();
            }
        }
    }

    if (reject) {
        sendReject(new PendingPrepare(request, observer, now), "LOW_BALLOT");
        return;
    }
    sendPromise(new PendingPrepare(request, observer, now));
    resetElectionTimer();
}


    private void sendPromise(PendingPrepare pending) {
        Ballot incoming = pending.request.getBallot();
        synchronized (this) {
            updateHighestPromisedBallot(incoming);
        }

        Promise.Builder promiseBuilder = Promise.newBuilder()
                .setOk(true)
                .setBallot(incoming)
                .addAllLog(snapshotKnownLog());

        PrepareResult result = PrepareResult.newBuilder()
                .setPromise(promiseBuilder.build())
                .build();
        pending.observer.onNext(result);
        pending.observer.onCompleted();
    }

    private void sendReject(PendingPrepare pending, String reason) {
        Ballot promised;
        synchronized (this) {
            promised = this.highestPromisedBallot;
        }

        Reject reject = Reject.newBuilder()
                .setPromisedBallot(promised)
                .setReason(reason)
                .build();
        PrepareResult result = PrepareResult.newBuilder().setReject(reject).build();
        pending.observer.onNext(result);
        pending.observer.onCompleted();
    }

    private final Map<Long, LogEntry> acceptedHistory = new ConcurrentHashMap<>();

    public synchronized void clearLeader(){
        this.leaderId=-1;
    }
    private synchronized List<LogEntry> snapshotKnownLog() {
        List<LogEntry> snapshot = new ArrayList<>();
        Set<Long> indices = new TreeSet<>(accepted.keySet());

        indices.addAll(accepted.keySet());
        indices.addAll(decided.keySet());
        for (Long idx : indices) {
            LogEntry entry = accepted.get(idx);
            if (entry == null) {
                entry = decided.get(idx);
            }
            if (entry != null) snapshot.add(entry);
        }
        return snapshot;
    }


    private void syncFromCluster() {
        long fromIndex;
        long localCommit;
        long localExec;
        synchronized (this) {
            localCommit = this.commitIndex;
            localExec = this.execIndex;
            long start = localExec + 1;
            long end = localCommit;
            long firstMissing = -1;
            for (long i = start; i <= end; i++) {
                if (!this.decided.containsKey(i)) { firstMissing = i; break; }
            }
            if (firstMissing == -1) {
                fromIndex = localCommit;
            } else {
                fromIndex = Math.max(0L, firstMissing - 1);
            }
        }

        List<String> targets = new ArrayList<>(peerStubs.keySet());
        targets.remove(this.nodeId);
        if (targets.isEmpty()) {
            return;
        }

        Collections.shuffle(targets, random);

        for (String target : targets) {
            PaxosServiceGrpc.PaxosServiceBlockingStub stub = peerStubs.get(target);
            if (stub == null) {
                continue;
            }
            try {
                SyncRequest req = SyncRequest.newBuilder()
                        .setNodeId(nodeId)
                        .setFromIndex(fromIndex)
                        .build();
                SyncResponse resp = stub.syncRPC(req);
                if (resp != null) {
                    applySyncResponse(resp);
                    //System.out.println("Node " + nodeId + ": Synchronized state from " + target);
                    return;
                }
            } catch (Exception e) {
                System.err.println("Node " + nodeId + ": Failed to sync from " + target + ". " + e.getMessage());
            }
        }

        System.err.println("Node " + nodeId + ": Unable to synchronize state from any peer.");
    }

    public synchronized SyncResponse buildSyncResponse(long fromIndex) {
        List<LogEntry> missing = new ArrayList<>();
        for (Long idx : new TreeSet<>(this.decided.keySet())) {
            if (idx > fromIndex) {
                missing.add(this.decided.get(idx));
            }
        }

        return SyncResponse.newBuilder()
                .addAllLog(missing)
                .setCommitIndex(this.commitIndex)
                .setLeaderBallot(this.currentBallot)
                .build();
    }

    private void applySyncResponse(SyncResponse response) {
        synchronized (this) {
            if (response.hasLeaderBallot()) {
                Ballot remoteBallot = response.getLeaderBallot();
                if (compareBallots(remoteBallot, this.currentBallot) > 0) {
                    this.currentBallot = remoteBallot;
                }
                updateHighestPromisedBallot(remoteBallot);
                this.leaderId = remoteBallot.getNodeId();
            }

            for (LogEntry entry : response.getLogList()) {
                LogEntry stored = LogEntry.newBuilder(entry).build();
                this.decided.put(stored.getIndex(), stored);
                //this.accepted.remove(stored.getIndex());
                this.commitIndex = Math.max(this.commitIndex, stored.getIndex());
            }
            this.nextLogIndex = Math.max(this.nextLogIndex, this.commitIndex + 1);
        }

        executeStateMachine();
    }

    public synchronized ViewDump buildViewDump() {
        ViewDump.Builder b = ViewDump.newBuilder();
        for (ViewChange vc : this.viewChangeHistory) b.addHistory(vc);
        return b.build();
    }



    public synchronized void handleViewChange(ViewChange request) {
        if (paused) return;
        
        Ballot newBallot = request.getBallot();
        this.leaderId = newBallot.getNodeId();
        this.isLeader = false;
        this.currentBallot = newBallot;
        this.highestPromisedBallot = newBallot;
        this.accepted.clear();
        long maxIndex = 0;
        for (LogEntry entry : request.getLogList()) {
            this.accepted.put(entry.getIndex(), entry);
            if (entry.getIndex() > maxIndex) {
                maxIndex = entry.getIndex();
            }
        }

        this.nextLogIndex = Math.max(maxIndex + 1, this.commitIndex + 1);
        this.viewChangeHistory.add(request);

//        System.out.println("Node " + nodeId + ": Adopted new view with " + request.getLogCount() +
//            " entries. Waiting for commits from leader.");

        resetElectionTimer();
    }

    public int compareBallots(Ballot b1, Ballot b2) {
        int roundCompare = Long.compare(b1.getRound(), b2.getRound());
        if (roundCompare != 0) return roundCompare;
        return Integer.compare(b1.getNodeId(), b2.getNodeId());
    }

    public String formatBallot(Ballot ballot) {
        if (ballot == null) return "N/A";
        return ballot.getRound() + "." + ballot.getNodeId();
    }

    private String formatTransaction(LogEntry entry) {
        if (entry.getIsNoop()) return "no-op";
        Transaction txn = entry.getTxn();
        return String.format("Txn(from=%s, to=%s, amt=%d)", txn.getSender(), txn.getReceiver(), txn.getAmount());
    }

    //--------------------------------------------------------------------------------
    // --- Print Methods --- I took the help of ChatGPT for generating CLI interface for processing transactions
    //--------------------------------------------------------------------------------

    public void printDB() {
        System.out.println("--- DB State for Node " + nodeId + " ---");
        this.balance.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
        System.out.println("-----------------------------");
    }

    public void printLog() {
        System.out.println("--- Log for Node " + nodeId + " (commit=" + commitIndex + ", exec=" + execIndex + ") ---");

        Set<Long> allIndices = new TreeSet<>(accepted.keySet());
        allIndices.addAll(decided.keySet());

        if (allIndices.isEmpty()) {
            System.out.println("Log is empty.");
        } else {
            allIndices.forEach(index -> {
                String status = getStatusForIndex(index);
                LogEntry entry = decided.getOrDefault(index, accepted.get(index));
                System.out.printf("Index %-3d: [Status=%s] [Ballot=%s] %s%n",
                        index, status, formatBallot(entry.getBallot()), formatTransaction(entry));
            });
        }
        System.out.println("-------------------------");
    }

    public void printStatus(long sequenceNumber) {
        String status = getStatusForIndex(sequenceNumber);
        System.out.printf("Node %s, Seq %d: %s%n", nodeId, sequenceNumber, status);
    }
    
    public void printStatusAll() {
        System.out.println("--- Status Overview for Node " + nodeId + " (commit=" + commitIndex + ") ---");
        if (commitIndex == 0) {
            System.out.println("No committed entries yet.");
        } else {
            for (long i = 1; i <= commitIndex; i++) {
                String status = getStatusForIndex(i);
                System.out.printf("Seq %d: %s%n", i, status);
            }
        }
        System.out.println("-------------------------");
    }

    public void printView() {
        System.out.println("--- ViewChange history for Node " + nodeId + " ---");
        if (this.viewChangeHistory.isEmpty()) {
            System.out.println("No NEW-VIEW messages recorded.");
        } else {
            int idx = 0;
            for (ViewChange vc : this.viewChangeHistory) {
                System.out.println("[#" + (++idx) + "] NEW-VIEW ballot=" + formatBallot(vc.getBallot()) +
                        ", entries=" + vc.getLogCount());
                for (LogEntry e : vc.getLogList()) {
                    String tx = e.getIsNoop() ? "noop" : formatTransaction(e);
                    System.out.println("  index=" + e.getIndex() + ", ballot=" + formatBallot(e.getBallot()) + ", " + tx);
                }
            }
        }
        System.out.println("----------------------------------------------");
    }

    public synchronized DBDump buildDBDump() {
        DBDump.Builder builder = DBDump.newBuilder()
                .setExecIndex(this.execIndex)
                .setCommitIndex(this.commitIndex)
                .setCurrentBallot(this.currentBallot);
        for (var entry : this.balance.entrySet()) {
            builder.putBalances(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    public synchronized LogDump buildLogDump() {
        java.util.Set<Long> indices = new java.util.TreeSet<>();
        indices.addAll(this.decided.keySet());
        indices.addAll(this.accepted.keySet());
        long maxIndex = this.commitIndex;
        if (!indices.isEmpty()) {
            maxIndex = Math.max(maxIndex, java.util.Collections.max(indices));
        }
        for (long i = 1; i <= maxIndex; i++) {
            indices.add(i);
        }

        LogDump.Builder dumpBuilder = LogDump.newBuilder();
        for (long idx : indices) {
            LogEntry entry = this.decided.getOrDefault(idx, this.accepted.get(idx));
            String status = getStatusForIndex(idx);

            LogRow.Builder row = LogRow.newBuilder()
                    .setIndex(idx)
                    .setStatus(status);

            if (entry != null) {
                row.setBallot(entry.getBallot())
                        .setIsNoop(entry.getIsNoop())
                        .setTxn(entry.getIsNoop() ? "no-op" : formatTransaction(entry));
            } else {
                row.setIsNoop(true)
                        .setTxn("missing");
            }
            dumpBuilder.addRows(row);
        }

        return dumpBuilder.build();
    }

    private String getStatusForIndex(long index) {
        if (index <= this.execIndex) return "E";
        if (this.decided.containsKey(index)) return "C";
        if (this.accepted.containsKey(index)) return "A";
        return "X";
    }

    private void loadSnapshotIfPresent() {
        try {
            Files.createDirectories(snapshotPath.getParent());
            if (!Files.exists(snapshotPath)) {
                if (csvImportEnabled) {
                    try {
                        loadCsvSnapshot();
                    } catch (Exception ex) {
                        System.err.println("Node " + nodeId + ": CSV import failed: " + ex.getMessage());
                    }
                }
                return;
            }
            List<String> lines = Files.readAllLines(snapshotPath);
            synchronized (this) {
                balance.clear();
                executedRequestIndex.clear();
                for (String line : lines) {
                    if (line == null || line.isBlank() || line.startsWith("#")) {
                        continue;
                    }
                    if (line.startsWith("execIndex=")) {
                        this.execIndex = Long.parseLong(line.substring("execIndex=".length()));
                        continue;
                    }
                    if (line.startsWith("commitIndex=")) {
                        this.commitIndex = Long.parseLong(line.substring("commitIndex=".length()));
                        continue;
                    }
                    if (line.startsWith("req:")) {
                        int eq = line.indexOf('=');
                        if (eq > 4) {
                            String key = line.substring(4, eq);
                            long idx = Long.parseLong(line.substring(eq + 1));
                            executedRequestIndex.put(key, idx);
                        }
                        continue;
                    }
                    int pos = line.indexOf('=');
                    if (pos > 0) {
                        String key = line.substring(0, pos);
                        int value = Integer.parseInt(line.substring(pos + 1));
                        balance.put(key, value);
                    }
                }
                this.nextLogIndex = Math.max(this.nextLogIndex, this.commitIndex + 1);
            }
            System.out.println("Node " + nodeId + ": Loaded snapshot.");
        } catch (Exception e) {
            System.err.println("Node " + nodeId + ": Snapshot load failed: " + e.getMessage());
        }
    }

    private void saveSnapshot() {
        try {
            Files.createDirectories(snapshotPath.getParent());
            List<String> out = new ArrayList<>();
            out.add("# simple kv snapshot");
            out.add("execIndex=" + this.execIndex);
            out.add("commitIndex=" + this.commitIndex);
            java.util.List<java.util.Map.Entry<String, Long>> reqEntries = new java.util.ArrayList<>(executedRequestIndex.entrySet());
            reqEntries.sort(java.util.Map.Entry.comparingByKey());
            for (var e : reqEntries) {
                out.add("req:" + e.getKey() + "=" + e.getValue());
            }
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(this.balance.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            for (Map.Entry<String, Integer> entry : entries) {
                out.add(entry.getKey() + "=" + entry.getValue());
            }

            Files.write(snapshotPath, out);
        if (csvExportEnabled) {
            try {
                writeCsvExport();
            } catch (Exception csvEx) {
                System.err.println("Node " + nodeId + ": CSV export failed: " + csvEx.getMessage());
            }
        }
    } catch (Exception e) {
        System.err.println("Node " + nodeId + ": Snapshot save failed: " + e.getMessage());
    }
}


    private void writeCsvExport() throws Exception {
        Path dbPath = snapshotPath.resolveSibling("snapshot-" + nodeId + ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(dbPath)) {
            writer.write("execIndex," + this.execIndex + "\n");
            writer.write("commitIndex," + this.commitIndex + "\n");
            List<Map.Entry<String,Long>> req = new ArrayList<>(executedRequestIndex.entrySet());
            req.sort(Map.Entry.comparingByKey());
            for (var e : req) writer.write("req," + e.getKey() + "=" + e.getValue() + "\n");
            List<Map.Entry<String,Integer>> bs = new ArrayList<>(balance.entrySet());
            bs.sort(Map.Entry.comparingByKey());
            for (var e : bs) writer.write(e.getKey() + "," + e.getValue() + "\n");
        }


        Path logPath = snapshotPath.resolveSibling("snapshot-log-" + nodeId + ".csv");
        try (BufferedWriter lw = Files.newBufferedWriter(logPath)) {
            lw.write("index,status,ballotRound,ballotNode,isNoop,clientId,ts,sender,receiver,amount\n");
            Set<Long> allIdx = new TreeSet<>();
            synchronized (this) {
                allIdx.addAll(accepted.keySet());
                allIdx.addAll(decided.keySet());
                long max = this.commitIndex;
                if (!allIdx.isEmpty()) max = Math.max(max, Collections.max(allIdx));
                for (long i = 1; i <= max; i++) allIdx.add(i);
                for (long idx : allIdx) {
                    String status = getStatusForIndex(idx); // E/C/A/X
                    LogEntry entry = this.decided.getOrDefault(idx, this.accepted.get(idx));
                    long br = 0; int bn = 0; boolean isNoop = false; String cid = ""; long ts = 0; String s = "", r = ""; int amt = 0;
                    if (entry != null) {
                        if (entry.hasBallot()) { br = entry.getBallot().getRound(); bn = entry.getBallot().getNodeId(); }
                        isNoop = entry.getIsNoop();
                        if (!isNoop && entry.hasTxn()) {
                            Transaction t = entry.getTxn();
                            cid = t.getClientId(); ts = t.getTimestamp(); s = t.getSender(); r = t.getReceiver(); amt = t.getAmount();
                        }
                    }
                    lw.write(idx + "," + status + "," + br + "," + bn + "," + isNoop + "," + cid + "," + ts + "," + s + "," + r + "," + amt + "\n");
                }
            }
        }
    }

    private void loadCsvSnapshot() throws Exception {
        Path dbPath = snapshotPath.resolveSibling("snapshot-" + nodeId + ".csv");
        if (Files.exists(dbPath)) {
            try (BufferedReader reader = Files.newBufferedReader(dbPath)) {
                String line;
                synchronized (this) {
                    balance.clear();
                    executedRequestIndex.clear();
                }
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank() || line.startsWith("#")) continue;
                    if (line.startsWith("execIndex,")) {
                        this.execIndex = Long.parseLong(line.substring("execIndex,".length()));
                        continue;
                    }
                    if (line.startsWith("commitIndex,")) {
                        this.commitIndex = Long.parseLong(line.substring("commitIndex,".length()));
                        continue;
                    }
                    if (line.startsWith("req,")) {
                        int eq = line.indexOf('=');
                        if (eq > 4) {
                            String key = line.substring(4, eq);
                            long idx = Long.parseLong(line.substring(eq + 1));
                            executedRequestIndex.put(key, idx);
                        }
                        continue;
                    }
                    int pos = line.indexOf(',');
                    if (pos > 0) {
                        String key = line.substring(0, pos);
                        int value = Integer.parseInt(line.substring(pos + 1));
                        balance.put(key, value);
                    }
                }
            }
        }


        Path logPath = snapshotPath.resolveSibling("snapshot-log-" + nodeId + ".csv");
        if (Files.exists(logPath)) {
            try (BufferedReader lr = Files.newBufferedReader(logPath)) {
                String line; boolean first = true;
                while ((line = lr.readLine()) != null) {
                    if (first) { first = false; continue; }
                    if (line.isBlank()) continue;
                    String[] cols = line.split(",", -1);
                    if (cols.length < 10) continue;
                    long idx = Long.parseLong(cols[0]);
                    String status = cols[1];
                    long br = Long.parseLong(cols[2]);
                    int bn = Integer.parseInt(cols[3]);
                    boolean isNoop = Boolean.parseBoolean(cols[4]);
                    String cid = cols[5];
                    long ts = 0; try { ts = Long.parseLong(cols[6]); } catch (Exception ignore) {}
                    String s = cols[7];
                    String r = cols[8];
                    int amt = 0; try { amt = Integer.parseInt(cols[9]); } catch (Exception ignore) {}

                    if ("C".equals(status) || "E".equals(status)) {
                        Ballot b = Ballot.newBuilder().setRound((int)br).setNodeId(bn).build();
                        LogEntry.Builder eb = LogEntry.newBuilder().setIndex(idx).setBallot(b).setIsNoop(isNoop);
                        if (!isNoop && cid != null && !cid.isEmpty()) {
                            Transaction t = Transaction.newBuilder()
                                    .setClientId(cid)
                                    .setTimestamp(ts)
                                    .setSender(s)
                                    .setReceiver(r)
                                    .setAmount(amt)
                                    .build();
                            eb.setTxn(t);
                        }
                        this.decided.put(idx, eb.build());
                        if (idx > this.commitIndex) this.commitIndex = idx;
                    }
                }
            }
        }

        synchronized (this) {
            this.nextLogIndex = Math.max(this.nextLogIndex, this.commitIndex + 1);
        }
        System.out.println("Node " + nodeId + ": Loaded CSV fallback.");
    }

}
