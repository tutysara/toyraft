package in.org.trident.toyraft.lib;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class RaftCluster {
    private final List<ConsensusModule> modules = new ArrayList<>();
    private final Map<Integer, Server> servers = new ConcurrentHashMap<>();
    //private volatile boolean networkPartitioned = false;
    private final Set<Integer> disconnectedServers = ConcurrentHashMap.newKeySet();

    public void createCluster(int size) {
        var peerIds = new ArrayList<Integer>();
        for (int i = 0; i < size; i++) {
            peerIds.add(i);
        }

        for (int i = 0; i < size; i++) {
            var otherPeers = new ArrayList<>(peerIds);
            otherPeers.remove(Integer.valueOf(i));

            int serverId = i;
            Server server = new Server() {
                @SuppressWarnings("unchecked")
                public <T, R> R call(int peerId, String method, T args) {
                    if (disconnectedServers.contains(serverId) ||
                            disconnectedServers.contains(peerId)) {
                        return null;
                    }

                    var targetModule = modules.get(peerId);
                    return switch (method) {
                        case "RequestVote" ->
                                (R) targetModule.requestVote((RequestVoteArgs) args);
                        case "AppendEntries" ->
                                (R) targetModule.appendEntries((AppendEntriesArgs) args);
                        default -> null;
                    };
                }
            };

            servers.put(i, server);
            var cm = new ConsensusModule(i, otherPeers, server);
            modules.add(cm);
        }

        modules.forEach(ConsensusModule::start);
    }

    public void disconnect(int serverId) {
        disconnectedServers.add(serverId);
    }

    public void reconnect(int serverId) {
        disconnectedServers.remove(serverId);
    }

    public void stop() {
        modules.forEach(ConsensusModule::stop);
    }

    public void stopServer(int serverId) {
        modules.get(serverId).stop();
    }

    public List<ConsensusModule.Report> getReports() {
        return modules.stream()
                .map(ConsensusModule::report)
                .toList();
    }

    public ConsensusModule getModule(int id) {
        return modules.get(id);
    }
}

// ==================== TEST CASES ====================


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RaftTests {

    private static final Logger log = LoggerFactory.getLogger(RaftTests.class);

    private RaftCluster cluster;

    @BeforeEach
    void setUp() {
        log.info("Setting up new test cluster");
        cluster = new RaftCluster();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            log.info("Tearing down cluster");
            cluster.stop();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Basic Election - Should elect exactly one leader")
    void testElectionBasic() throws InterruptedException {
        log.info("Testing basic election");
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(ConsensusModule.Report::isLeader).count();

        log.info("Leader count: {}", leaderCount);
        assertEquals(1, leaderCount, "Expected exactly one leader");

        int leaderTerm = reports.stream()
                .filter(ConsensusModule.Report::isLeader)
                .findFirst()
                .orElseThrow()
                .term();

        boolean allSameTerm = reports.stream()
                .allMatch(r -> r.term() == leaderTerm);

        assertTrue(allSameTerm, "All servers should be in the same term");
        log.info("✓ Basic election test passed");
    }

    @Test
    @Order(2)
    @DisplayName("Leader Disconnect - Should elect new leader when current leader disconnects")
    void testElectionLeaderDisconnect() throws InterruptedException {
        log.info("Testing leader disconnect");
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        int leaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .orElseThrow()
                .id();

        int initialTerm = reports.get(leaderId).term();
        log.info("Initial leader: {} at term {}", leaderId, initialTerm);

        cluster.disconnect(leaderId);
        log.info("Disconnected leader {}", leaderId);

        Thread.sleep(1500);

        reports = cluster.getReports();
        long leaderCount = reports.stream()
                .filter(r -> r.isLeader() && r.id() != leaderId)
                .count();

        log.info("New leader count: {}", leaderCount);
        assertEquals(1, leaderCount, "Expected exactly one new leader");

        var newLeader = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .orElseThrow();

        log.info("New leader: {} at term {}", newLeader.id(), newLeader.term());
        assertTrue(newLeader.term() > initialTerm, "New leader should have higher term");
        log.info("✓ Leader disconnect test passed");
    }

    @Test
    @Order(3)
    @DisplayName("Leader Reconnect - Old leader should become follower when reconnecting")
    void testElectionLeaderReconnect() throws InterruptedException {
        log.info("Testing leader reconnect");
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        int oldLeaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .orElseThrow()
                .id();

        log.info("Old leader: {}", oldLeaderId);

        cluster.disconnect(oldLeaderId);
        Thread.sleep(1500);

        reports = cluster.getReports();
        int newLeaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .orElseThrow()
                .id();

        log.info("New leader after disconnect: {}", newLeaderId);

        cluster.reconnect(oldLeaderId);
        Thread.sleep(1000);

        reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        log.info("Leader count after reconnect: {}", leaderCount);
        assertEquals(1, leaderCount, "Should still have exactly one leader");

        boolean oldLeaderIsFollower = reports.stream()
                .filter(r -> r.id() == oldLeaderId)
                .noneMatch(r -> r.isLeader());

        assertTrue(oldLeaderIsFollower, "Old leader should become follower");
        log.info("✓ Leader reconnect test passed");
    }

    @Test
    @Order(4)
    @DisplayName("No Quorum - Should not elect leader without quorum")
    void testElectionNoQuorum() throws InterruptedException {
        log.info("Testing no quorum scenario");
        cluster.createCluster(3);

        Thread.sleep(1000);

        cluster.disconnect(0);
        cluster.disconnect(1);

        Thread.sleep(1500);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        log.info("Leader count with no quorum: {}", leaderCount);
        assertEquals(0, leaderCount, "Should have no leader without quorum");

        cluster.reconnect(0);
        Thread.sleep(1500);

        reports = cluster.getReports();
        leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        log.info("Leader count after restoring quorum: {}", leaderCount);
        assertEquals(1, leaderCount, "Should elect leader after quorum restored");
        log.info("✓ No quorum test passed");
    }

    @Test
    @Order(5)
    @DisplayName("Multiple Terms - Term should increase with each election")
    void testElectionMultipleTerms() throws InterruptedException {
        log.info("Testing multiple elections");
        cluster.createCluster(5);

        Thread.sleep(1000);

        var initialReports = cluster.getReports();
        int initialTerm = initialReports.get(0).term();
        log.info("Initial term: {}", initialTerm);

        for (int i = 0; i < 3; i++) {
            var reports = cluster.getReports();
            int leaderId = reports.stream()
                    .filter(r -> r.isLeader())
                    .findFirst()
                    .orElseThrow()
                    .id();

            log.info("Round {}: Stopping leader {}", i + 1, leaderId);
            cluster.stopServer(leaderId);

            Thread.sleep(1500);

            reports = cluster.getReports();
            long leaderCount = reports.stream()
                    .filter(r -> r.isLeader())
                    .count();

            assertEquals(1, leaderCount,
                    String.format("Should have exactly one leader in round %d", i + 1));

            int currentTerm = reports.stream()
                    .filter(ConsensusModule.Report::isLeader)
                    .findFirst()
                    .orElseThrow()
                    .term();

            log.info("New term: {}", currentTerm);
            assertTrue(currentTerm > initialTerm, "Term should increase");
        }

        log.info("✓ Multiple elections test passed");
    }

    @Test
    @Order(6)
    @DisplayName("Large Cluster - Should elect leader in 7-server cluster")
    void testElectionLargeCluster() throws InterruptedException {
        log.info("Testing large cluster (7 servers)");
        cluster.createCluster(7);

        Thread.sleep(1500);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        log.info("Leader count in 7-server cluster: {}", leaderCount);
        assertEquals(1, leaderCount, "Should have exactly one leader");

        int leaderTerm = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .orElseThrow()
                .term();

        boolean allSameTerm = reports.stream()
                .allMatch(r -> r.term() == leaderTerm);

        assertTrue(allSameTerm, "All servers should converge to same term");
        log.info("✓ Large cluster test passed");
    }

    @Test
    @Order(7)
    @DisplayName("Concurrent Leader Failures - Should handle rapid successive failures")
    void testConcurrentLeaderFailures() throws InterruptedException {
        log.info("Testing concurrent leader failures");
        cluster.createCluster(5);

        Thread.sleep(1000);

        // Rapidly disconnect and reconnect leaders
        for (int round = 0; round < 2; round++) {
            var reports = cluster.getReports();
            var leader = reports.stream()
                    .filter(r -> r.isLeader())
                    .findFirst();

            if (leader.isPresent()) {
                int leaderId = leader.get().id();
                log.info("Round {}: Disconnecting leader {}", round, leaderId);
                cluster.disconnect(leaderId);
                Thread.sleep(500);
                cluster.reconnect(leaderId);
            }
        }

        Thread.sleep(1500);

        var finalReports = cluster.getReports();
        long leaderCount = finalReports.stream().filter(r -> r.isLeader()).count();

        assertEquals(1, leaderCount,
                "Should stabilize to exactly one leader after concurrent failures");
        log.info("✓ Concurrent failures test passed");
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Minimum Cluster - 1 server should become leader immediately")
        void testMinimumCluster() throws InterruptedException {
            log.info("Testing minimum cluster (1 server)");
            cluster.createCluster(1);

            Thread.sleep(500);

            var reports = cluster.getReports();
            long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

            assertEquals(1, leaderCount, "Single server should elect itself as leader");
            log.info("✓ Minimum cluster test passed");
        }

        @Test
        @DisplayName("Even Cluster - Should work with even number of servers")
        void testEvenCluster() throws InterruptedException {
            log.info("Testing even cluster (4 servers)");
            cluster.createCluster(4);

            Thread.sleep(1000);

            var reports = cluster.getReports();
            long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

            assertEquals(1, leaderCount, "Even cluster should elect exactly one leader");
            log.info("✓ Even cluster test passed");
        }
    }
}
