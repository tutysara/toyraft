package in.org.trident.toyraft.lib;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
        import java.util.concurrent.*;
        import java.util.concurrent.locks.ReentrantLock;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Records for RPC messages
record RequestVoteArgs(
        int term,
        int candidateId,
        int lastLogIndex,
        int lastLogTerm
) {}

record RequestVoteReply(
        int term,
        boolean voteGranted
) {}

record AppendEntriesArgs(
        int term,
        int leaderId,
        int prevLogIndex,
        int prevLogTerm,
        List<LogEntry> entries,
        int leaderCommit
) {}

record AppendEntriesReply(
        int term,
        boolean success
) {}

record LogEntry(
        int term,
        Object command
) {}

enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    DEAD
}

// Main Consensus Module
public class ConsensusModule {
    private static final Logger logger = LoggerFactory.getLogger(ConsensusModule.class);

    private final int id;
    private final List<Integer> peerIds;
    private final Server server;
    private final ReentrantLock lock = new ReentrantLock();

    // Persistent state
    private int currentTerm = 0;
    private Integer votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // Volatile state
    private int commitIndex = -1;
    private int lastApplied = -1;
    private State state = State.FOLLOWER;
    private Instant lastHeartbeat = Instant.now();

    // Leader state
    private final Map<Integer, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> matchIndex = new ConcurrentHashMap<>();

    private final RandomGenerator random = RandomGenerator.getDefault();
    private volatile boolean running = true;

    public ConsensusModule(int id, List<Integer> peerIds, Server server) {
        this.id = id;
        this.peerIds = new ArrayList<>(peerIds);
        this.server = server;
    }

    public void start() {
        Thread.ofVirtual().start(this::electionTimerLoop);
    }

    private void electionTimerLoop() {
        while (running) {
            try {
                Thread.sleep(10);

                lock.lock();
                try {
                    if (state == State.DEAD) {
                        return;
                    }

                    if (state != State.LEADER &&
                            Duration.between(lastHeartbeat, Instant.now()).toMillis() > electionTimeout()) {
                        startElection();
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private int electionTimeout() {
        return 150 + random.nextInt(150);
    }

    private void startElection() {
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = id;
        lastHeartbeat = Instant.now();

        int savedTerm = currentTerm;

        logger.debug("[Server-{}] Starting election for term {}", id, currentTerm);

        Thread.ofVirtual().start(() -> {
            try (var scope = StructuredTaskScope.open()) {
                var voteFutures = new ArrayList<StructuredTaskScope.Subtask<RequestVoteReply>>();

                for (int peerId : peerIds) {
                    int lastLogIdx = log.size() - 1;
                    int lastLogTrm = lastLogIdx >= 0 ? log.get(lastLogIdx).term() : 0;

                    var args = new RequestVoteArgs(savedTerm, id, lastLogIdx, lastLogTrm);
                    var future = scope.fork(() ->
                            server.<RequestVoteArgs, RequestVoteReply>call(peerId, "RequestVote", args));
                    voteFutures.add(future);
                }

                scope.join();

                int votesReceived = 1;

                for (var future : voteFutures) {
                    try {
                        var reply = future.get();
                        if (reply != null) {
                            lock.lock();
                            try {
                                if (reply.term() > savedTerm) {
                                    logger.debug("[Server-{}] Term out of date in RequestVoteReply", id);
                                    becomeFollower(reply.term());
                                    return;
                                }
                            } finally {
                                lock.unlock();
                            }

                            if (reply.voteGranted()) {
                                votesReceived++;
                            }
                        }
                    } catch (Exception e) {
                        // Peer didn't respond
                    }
                }

                lock.lock();
                try {
                    if (state != State.CANDIDATE || currentTerm != savedTerm) {
                        logger.debug("[Server-{}] State changed while waiting for votes", id);
                        return;
                    }

                    if (votesReceived * 2 > peerIds.size() + 1) {
                        logger.info("[Server-{}] Won election with {} votes", id, votesReceived);
                        becomeLeader();
                    }
                } finally {
                    lock.unlock();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void becomeLeader() {
        state = State.LEADER;
        logger.info("[Server-{}] Became leader for term {}", id, currentTerm);

        for (int peerId : peerIds) {
            nextIndex.put(peerId, log.size());
            matchIndex.put(peerId, -1);
        }

        Thread.ofVirtual().start(this::leaderHeartbeatLoop);
    }

    private void leaderHeartbeatLoop() {
        while (running) {
            try {
                sendHeartbeats();
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            lock.lock();
            try {
                if (state != State.LEADER) {
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void sendHeartbeats() {
        lock.lock();
        int savedTerm = currentTerm;
        lock.unlock();

        for (int peerId : peerIds) {
            Thread.ofVirtual().start(() -> {
                lock.lock();
                int ni = nextIndex.getOrDefault(peerId, 0);
                int prevLogIdx = ni - 1;
                int prevLogTrm = prevLogIdx >= 0 ? log.get(prevLogIdx).term() : 0;
                var entries = log.subList(ni, log.size());

                var args = new AppendEntriesArgs(
                        savedTerm, id, prevLogIdx, prevLogTrm,
                        new ArrayList<>(entries), commitIndex
                );
                lock.unlock();

                try {
                    var reply = server.<AppendEntriesArgs, AppendEntriesReply>call(peerId, "AppendEntries", args);
                    if (reply != null) {
                        lock.lock();
                        try {
                            if (reply.term() > savedTerm) {
                                becomeFollower(reply.term());
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception e) {
                    // Peer didn't respond
                }
            });
        }
    }

    private void becomeFollower(int term) {
        logger.debug("[Server-{}] Becoming follower for term {}", id, term);
        state = State.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        lastHeartbeat = Instant.now();
    }

    public RequestVoteReply requestVote(RequestVoteArgs args) {
        lock.lock();
        try {
            if (state == State.DEAD) {
                return null;
            }

            int lastLogIdx = log.size() - 1;
            int lastLogTrm = lastLogIdx >= 0 ? log.get(lastLogIdx).term() : 0;

            logger.debug("[Server-{}] RequestVote: {} [currentTerm={}, votedFor={}, log index/term=({}, {})]",
                    id, args, currentTerm, votedFor, lastLogIdx, lastLogTrm);

            if (args.term() > currentTerm) {
                becomeFollower(args.term());
            }

            if (args.term() == currentTerm &&
                    (votedFor == null || votedFor == args.candidateId()) &&
                    (args.lastLogTerm() > lastLogTrm ||
                            (args.lastLogTerm() == lastLogTrm && args.lastLogIndex() >= lastLogIdx))) {

                votedFor = args.candidateId();
                lastHeartbeat = Instant.now();
                return new RequestVoteReply(currentTerm, true);
            }

            return new RequestVoteReply(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    public AppendEntriesReply appendEntries(AppendEntriesArgs args) {
        lock.lock();
        try {
            if (state == State.DEAD) {
                return null;
            }

            logger.debug("[Server-{}] AppendEntries: {}", id, args);

            if (args.term() > currentTerm) {
                becomeFollower(args.term());
            }

            if (args.term() == currentTerm) {
                if (state != State.FOLLOWER) {
                    becomeFollower(args.term());
                }
                lastHeartbeat = Instant.now();
                return new AppendEntriesReply(currentTerm, true);
            }

            return new AppendEntriesReply(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        lock.lock();
        try {
            state = State.DEAD;
            running = false;
            logger.info("[Server-{}] Stopped", id);
        } finally {
            lock.unlock();
        }
    }

    public record Report(
            int id,
            int term,
            boolean isLeader
    ) {}

    public Report report() {
        lock.lock();
        try {
            return new Report(id, currentTerm, state == State.LEADER);
        } finally {
            lock.unlock();
        }
    }
}
