package in.org.trident.toyraft.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

interface Server {
    <T, R> R call(int peerId, String method, T args);
}

class RaftCluster {
    private final List<ConsensusModule> modules = new ArrayList<>();
    private final Map<Integer, Server> servers = new ConcurrentHashMap<>();
    private volatile boolean networkPartitioned = false;
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

public class RaftTests {

    static void testElectionBasic() throws InterruptedException {
        System.out.println("\n=== Testing Basic Election ===");
        var cluster = new RaftCluster();
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        System.out.println("Leader count: " + leaderCount);
        assert leaderCount == 1 : "Expected exactly one leader";

        int leaderTerm = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get()
                .term();

        boolean allSameTerm = reports.stream()
                .allMatch(r -> r.term() == leaderTerm);

        assert allSameTerm : "All servers should be in the same term";

        cluster.stop();
        System.out.println("✓ Basic election test passed");
    }

    static void testElectionLeaderDisconnect() throws InterruptedException {
        System.out.println("\n=== Testing Leader Disconnect ===");
        var cluster = new RaftCluster();
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        int leaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get()
                .id();

        int initialTerm = reports.get(leaderId).term();
        System.out.println("Initial leader: " + leaderId + " at term " + initialTerm);

        cluster.disconnect(leaderId);
        System.out.println("Disconnected leader " + leaderId);

        Thread.sleep(1500);

        reports = cluster.getReports();
        long leaderCount = reports.stream()
                .filter(r -> r.isLeader() && r.id() != leaderId)
                .count();

        System.out.println("New leader count: " + leaderCount);
        assert leaderCount == 1 : "Expected exactly one new leader";

        var newLeader = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get();

        System.out.println("New leader: " + newLeader.id() + " at term " + newLeader.term());
        assert newLeader.term() > initialTerm : "New leader should have higher term";

        cluster.stop();
        System.out.println("✓ Leader disconnect test passed");
    }

    static void testElectionLeaderReconnect() throws InterruptedException {
        System.out.println("\n=== Testing Leader Reconnect ===");
        var cluster = new RaftCluster();
        cluster.createCluster(3);

        Thread.sleep(1000);

        var reports = cluster.getReports();
        int oldLeaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get()
                .id();

        System.out.println("Old leader: " + oldLeaderId);

        cluster.disconnect(oldLeaderId);
        Thread.sleep(1500);

        reports = cluster.getReports();
        int newLeaderId = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get()
                .id();

        System.out.println("New leader after disconnect: " + newLeaderId);

        cluster.reconnect(oldLeaderId);
        Thread.sleep(1000);

        reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        System.out.println("Leader count after reconnect: " + leaderCount);
        assert leaderCount == 1 : "Should still have exactly one leader";

        boolean oldLeaderIsFollower = reports.stream()
                .filter(r -> r.id() == oldLeaderId)
                .noneMatch(r -> r.isLeader());

        assert oldLeaderIsFollower : "Old leader should become follower";

        cluster.stop();
        System.out.println("✓ Leader reconnect test passed");
    }

    static void testElectionNoQuorum() throws InterruptedException {
        System.out.println("\n=== Testing No Quorum ===");
        var cluster = new RaftCluster();
        cluster.createCluster(3);

        Thread.sleep(1000);

        cluster.disconnect(0);
        cluster.disconnect(1);

        Thread.sleep(1500);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        System.out.println("Leader count with no quorum: " + leaderCount);
        assert leaderCount == 0 : "Should have no leader without quorum";

        cluster.reconnect(0);
        Thread.sleep(1500);

        reports = cluster.getReports();
        leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        System.out.println("Leader count after restoring quorum: " + leaderCount);
        assert leaderCount == 1 : "Should elect leader after quorum restored";

        cluster.stop();
        System.out.println("✓ No quorum test passed");
    }

    static void testElectionMultipleTerms() throws InterruptedException {
        System.out.println("\n=== Testing Multiple Elections ===");
        var cluster = new RaftCluster();
        cluster.createCluster(5);

        Thread.sleep(1000);

        var initialReports = cluster.getReports();
        int initialTerm = initialReports.get(0).term();
        System.out.println("Initial term: " + initialTerm);

        for (int i = 0; i < 3; i++) {
            var reports = cluster.getReports();
            int leaderId = reports.stream()
                    .filter(r -> r.isLeader())
                    .findFirst()
                    .get()
                    .id();

            System.out.println("Round " + (i+1) + ": Stopping leader " + leaderId);
            cluster.stopServer(leaderId);

            Thread.sleep(1500);

            reports = cluster.getReports();
            long leaderCount = reports.stream()
                    .filter(r -> r.isLeader())
                    .count();

            assert leaderCount == 1 : "Should have exactly one leader in round " + (i+1);

            // no leader when there are only 2 nodes?
            int currentTerm = reports.stream()
                    .filter(r -> r.isLeader())
                    .findFirst()
                    .get()
                    .term();

            System.out.println("New term: " + currentTerm);
            assert currentTerm > initialTerm : "Term should increase";
        }

        cluster.stop();
        System.out.println("✓ Multiple elections test passed");
    }

    static void testElectionLargCluster() throws InterruptedException {
        System.out.println("\n=== Testing Large Cluster (7 servers) ===");
        var cluster = new RaftCluster();
        cluster.createCluster(7);

        Thread.sleep(1500);

        var reports = cluster.getReports();
        long leaderCount = reports.stream().filter(r -> r.isLeader()).count();

        System.out.println("Leader count in 7-server cluster: " + leaderCount);
        assert leaderCount == 1 : "Should have exactly one leader";

        int leaderTerm = reports.stream()
                .filter(r -> r.isLeader())
                .findFirst()
                .get()
                .term();

        boolean allSameTerm = reports.stream()
                .allMatch(r -> r.term() == leaderTerm);

        assert allSameTerm : "All servers should converge to same term";

        cluster.stop();
        System.out.println("✓ Large cluster test passed");
    }

    public static void main(String[] args) {
        try {
            testElectionBasic();
            testElectionLeaderDisconnect();
            testElectionLeaderReconnect();
            testElectionNoQuorum();
            testElectionMultipleTerms();
            testElectionLargCluster();

            System.out.println("\n" + "=".repeat(50));
            System.out.println("All tests passed! ✓");
            System.out.println("=".repeat(50));

        } catch (Exception e) {
            System.err.println("\n✗ Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}