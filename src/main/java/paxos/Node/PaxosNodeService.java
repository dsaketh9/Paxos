package paxos.Node;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import paxos.rpc.*;

public class PaxosNodeService extends PaxosServiceGrpc.PaxosServiceImplBase {

    private final Node paxosNode;
    private static final int CLIENT_FORWARD_DEADLINE_MS = 1500;
    public PaxosNodeService(Node paxosNode) {
        this.paxosNode = paxosNode;
    }

    @Override
    public void prepareRPC(Prepare request, StreamObserver<PrepareResult> responseObserver) {
        paxosNode.handlePrepare(request, responseObserver);
    }

    @Override
    public void acceptRPC(Accept req, StreamObserver<AcceptResult> obs) {
        if (paxosNode.isPaused()) {
            obs.onError(Status.UNAVAILABLE.withDescription("NODE_PAUSED").asRuntimeException());
            return;
        }

        Ballot promised = paxosNode.getHighestPromisedBallot();
        if (paxosNode.compareBallots(req.getBallot(), promised) < 0) {
            obs.onNext(AcceptResult.newBuilder()
                    .setReject(Reject.newBuilder().setPromisedBallot(promised).build())
                    .build());
            obs.onCompleted();
            return;
        }


        paxosNode.updateHighestPromisedBallot(req.getBallot());
        LogEntry entry = LogEntry.newBuilder()
                .setBallot(req.getBallot())
                .setIndex(req.getIndex())
                .setTxn(req.getTxn())
                .setIsNoop(req.getIsNoop())
                .build();
        paxosNode.addToAcceptedLog(entry);

        paxosNode.resetElectionTimer();

        obs.onNext(AcceptResult.newBuilder()
                .setAccepted(Accepted.newBuilder().setOk(true).build()).build());
        obs.onCompleted();
    }



    @Override
    public void commitRPC(Commit req, StreamObserver<CommitAck> obs) {
        if (paxosNode.isPaused()) {
            obs.onError(Status.UNAVAILABLE.withDescription("NODE_PAUSED").asRuntimeException());
            return;
        }
        paxosNode.handleCommit(req);
        paxosNode.resetElectionTimer();
        obs.onNext(CommitAck.newBuilder().setAppliedUpToIndex(paxosNode.getCommitIndex()).build());
        obs.onCompleted();
    }

    @Override
    public void clientRequestRPC(ClientRequest req, StreamObserver<ClientReply> obs) {
        if (paxosNode.isPaused()) {
            obs.onError(Status.UNAVAILABLE.withDescription("NODE_PAUSED").asRuntimeException());
            return;
        }

        if (paxosNode.isLeader()) {
            ClientReply reply = paxosNode.handleClientRequest(req.getTxn());
            obs.onNext(reply);
            obs.onCompleted();
            return;
        }

        int leaderId = paxosNode.getLeaderId();
        if (leaderId == -1) {
            paxosNode.enqueueClientRequest(req.getTxn());
            paxosNode.maybeStartElectionOnClientRequest();
            TxnReply reply = TxnReply.newBuilder()
                    .setSuccess(false)
                    .setMessage("ELECTION_IN_PROGRESS: queued; please retry")
                    .build();
            obs.onNext(ClientReply.newBuilder().setReply(reply).build());
            obs.onCompleted();
            return;
        }

        String leaderNodeId = "n" + leaderId;

        if (leaderNodeId.equals(paxosNode.getNodeId())) {
            paxosNode.markLeaderUnknownAndTriggerElection();
            paxosNode.enqueueClientRequest(req.getTxn());
            TxnReply reply = TxnReply.newBuilder()
                    .setSuccess(false)
                    .setMessage("LEADER_TRANSITION: electing new leader; request queued")
                    .build();
            obs.onNext(ClientReply.newBuilder().setReply(reply).build());
            obs.onCompleted();
            return;
        }

        PaxosServiceGrpc.PaxosServiceBlockingStub leaderStub = paxosNode.getOrCreateStub(leaderNodeId);

        if (leaderStub == null) {
            TxnReply reply = TxnReply.newBuilder()
                    .setSuccess(false)
                    .setMessage("INTERNAL_ERROR: Leader stub not found for " + leaderNodeId)
                    .build();
            obs.onNext(ClientReply.newBuilder().setReply(reply).build());
            obs.onCompleted();
            return;
        }

        try {
            System.out.println("Node " + paxosNode.getNodeId() + ": Not the leader. Forwarding request to " + leaderNodeId);
            ClientReply leaderReply = leaderStub
                    .withDeadlineAfter(CLIENT_FORWARD_DEADLINE_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .clientRequestRPC(req);
            obs.onNext(leaderReply);
            obs.onCompleted();
        } catch (Exception e) {
            System.err.println("Node " + paxosNode.getNodeId() + ": Failed to forward request to leader " + leaderNodeId + ". " + e.getMessage());
            paxosNode.markLeaderUnknownAndTriggerElection();
            paxosNode.enqueueClientRequest(req.getTxn());
            TxnReply reply = TxnReply.newBuilder()
                    .setSuccess(false)
                    .setMessage("FORWARDING_ERROR: Could not contact leader. Please retry.")
                    .build();
            obs.onNext(ClientReply.newBuilder().setReply(reply).build());
            obs.onCompleted();
        }
    }


    @Override
    public void viewChangeRPC(ViewChange req, StreamObserver<ViewChangeAck> resp) {
        ViewChangeAck.Builder ack = ViewChangeAck.newBuilder();

        if (paxosNode.compareBallots(req.getBallot(), paxosNode.getHighestPromisedBallot()) < 0) {
            ack.setOk(false)
                    .setPromisedBallot(paxosNode.getHighestPromisedBallot())
                    .setReason("LOW_BALLOT");
            resp.onNext(ack.build());
            resp.onCompleted();
            return;
        }


        paxosNode.handleViewChange(req);


        for (LogEntry m : req.getLogList()) {
            ack.addAcceptedIndices(m.getIndex());
        }
        ack.setOk(true);
        resp.onNext(ack.build());
        resp.onCompleted();
    }


    @Override
    public void syncRPC(SyncRequest req, StreamObserver<SyncResponse> obs) {
        SyncResponse response = paxosNode.buildSyncResponse(req.getFromIndex());
        obs.onNext(response);
        obs.onCompleted();
    }
    
    @Override
    public void ping(paxos.rpc.PingRequest req, io.grpc.stub.StreamObserver<paxos.rpc.PingResponse> obs) {
        if (paxosNode.isPaused()) {
            obs.onError(Status.UNAVAILABLE.withDescription("NODE_PAUSED").asRuntimeException());
            return;
        }

        paxosNode.resetElectionTimer();
        obs.onNext(paxos.rpc.PingResponse.newBuilder()
            .setTo(req.getFrom())
            .setOk("ACK")
            .build());
        obs.onCompleted();
    }

    @Override
    public void pauseNodeRPC(paxos.rpc.PauseRequest req, io.grpc.stub.StreamObserver<paxos.rpc.PauseResponse> obs) {
        paxosNode.pauseNode();
        obs.onNext(paxos.rpc.PauseResponse.newBuilder().setPaused(true).build());
        obs.onCompleted();
    }

    @Override
    public void resumeNodeRPC(paxos.rpc.ResumeRequest req, io.grpc.stub.StreamObserver<paxos.rpc.ResumeResponse> obs) {
        paxosNode.resumeNode();
        obs.onNext(paxos.rpc.ResumeResponse.newBuilder().setResumed(true).build());
        obs.onCompleted();
    }


    @Override
    public void getView(com.google.protobuf.Empty req, StreamObserver<ViewDump> obs) {
        ViewDump dump = paxosNode.buildViewDump();
        obs.onNext(dump);
        obs.onCompleted();
    }

    @Override
    public void getDB(Empty req, StreamObserver<DBDump> obs) {
        DBDump dump = paxosNode.buildDBDump();
        obs.onNext(dump);
        obs.onCompleted();
    }

    @Override
    public void getLog(Empty req, StreamObserver<LogDump> obs) {
        LogDump dump = paxosNode.buildLogDump();
        obs.onNext(dump);
        obs.onCompleted();
    }
}
