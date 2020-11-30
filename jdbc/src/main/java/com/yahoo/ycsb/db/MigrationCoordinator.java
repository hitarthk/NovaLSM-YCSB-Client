package com.yahoo.ycsb.db;

import java.util.*;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.db.ConfigurationUtil.Configurations;
import com.yahoo.ycsb.db.ConfigurationUtil.Configuration;
import com.yahoo.ycsb.db.beans.LogReplaySummary;
import com.yahoo.ycsb.db.beans.MigrationSummary;

public class MigrationCoordinator {

    // These should exactly match the contents in NovaGlobalVariables in nova_common.h
    static final char MIGRATION_INIT_PHASE = 'F';
    static final char MIGRATION_INTERMEDIATE_PHASE = 'S';
    static final char MIGRATION_CONCLUDE_PHASE = 'L';

    static long BYTES_THRESHOLD = 16000000;

    private static MigrationSummary getMigrationSummary(Configurations configs, int currentConfigId, int newConfigId) {
        Map<Integer, List<Long>> destinationFragmentsMap = new HashMap<>();
        Map<Integer, List<Long>> sourceFragmentsMap = new HashMap<>();
        Map<Integer, List<Integer>> fragmentsToSrcDst = new HashMap<>();
        Configuration currentConfig = configs.configs.get(currentConfigId);
        Configuration newConfig = configs.configs.get(newConfigId);
        assert currentConfig.fragments.size() == newConfig.fragments.size();
        for (int i = 0; i < currentConfig.fragments.size(); i++) {
            LTCFragment currentFrag = currentConfig.fragments.get(i);
            LTCFragment newFrag = newConfig.fragments.get(i);
            System.out.println("Fragment ids: " + currentFrag.dbId + "|" + newFrag.dbId + " ltcServer ids: " + currentFrag.ltcServerId + "|" + newFrag.ltcServerId);
            if (currentFrag.ltcServerId != newFrag.ltcServerId) {
                List<Long> migratedFragments = destinationFragmentsMap.getOrDefault(newFrag.ltcServerId, new ArrayList<Long>());
                migratedFragments.add((long) i);
                destinationFragmentsMap.put(newFrag.ltcServerId, migratedFragments);

                migratedFragments = sourceFragmentsMap.getOrDefault(currentFrag.ltcServerId, new ArrayList<>());
                migratedFragments.add((long) i);
                sourceFragmentsMap.put(currentFrag.ltcServerId, migratedFragments);

                List<Integer> srcDst = new ArrayList<Integer>() {{
                    add(currentFrag.ltcServerId);
                    add(newFrag.ltcServerId);
                }};
                fragmentsToSrcDst.put(i, srcDst);
            }
        }

        MigrationSummary summary = new MigrationSummary(destinationFragmentsMap, sourceFragmentsMap, fragmentsToSrcDst);
        System.out.println(summary);
        return summary;
    }

    public static void executeInitPhase(NovaClient client, int nservers) {
        for (int serverId = 0; serverId < nservers; serverId++) {
            client.requestSourceToSendData(serverId, MIGRATION_INIT_PHASE);
        }
    }

    public static void executeIntermediatePhase(NovaClient client, MigrationSummary summary, long thresholdForHandover) throws InterruptedException {
        Map<Integer, LogReplaySummary> migrationState = new HashMap<>();
        for (Integer migratingFragment : summary.getFragmentsToSrcDst().keySet()) {
            migrationState.put(migratingFragment, new LogReplaySummary());
        }

        int totalMigratingFragments = summary.getFragmentsToSrcDst().size();
        Set<Integer> doneFragments = new HashSet<>();
        boolean done = false;
        while (doneFragments.size() < totalMigratingFragments) {
            Set<Integer> fragsWithChangedIteration = new HashSet<>();
            for (Map.Entry<Integer, List<Integer>> e : summary.getFragmentsToSrcDst().entrySet()) {
                Integer migratingFragment = e.getKey();
                if (!doneFragments.contains(migratingFragment)) {
                    Integer sourceServer = e.getValue().get(0);
                    Integer destinationServer = e.getValue().get(1);
                    NovaClient.ReturnValue v = client.getLogRecordsReplayed(destinationServer, migratingFragment);
                    LogReplaySummary newSummary = LogReplaySummary.parse(v.getValue);
                    System.out.println("Fragment " + migratingFragment + " old: " + migrationState.get(migratingFragment) +
                            " | new: " + newSummary);
                    if (migrationState.get(migratingFragment).getIteration() != newSummary.getIteration()) {
                        if (newSummary.getBytesReplayed() > thresholdForHandover) {
                            migrationState.put(migratingFragment, newSummary);
                            fragsWithChangedIteration.add(migratingFragment);
                        } else {
                            doneFragments.add(migratingFragment);
                        }
                    }
                }
            }

            System.out.println("Fragments with changed iteration: " + fragsWithChangedIteration);

            for (Integer fragment : fragsWithChangedIteration) {
                Integer sourceServer = summary.getFragmentsToSrcDst().get(fragment).get(0);
                client.requestSourceToSendData(sourceServer, MIGRATION_INTERMEDIATE_PHASE);
            }

            Thread.sleep(1000);
        }
    }

    public static void executeConcludePhase(NovaClient client, int nservers) throws InterruptedException {
        for (int serverId = 0; serverId < nservers; serverId++) {
            client.requestSourceToSendData(serverId, MIGRATION_CONCLUDE_PHASE);
        }

        while (true) {
            boolean isAllReady = true;
            for (int i = 0; i < nservers; i++) {
                boolean isReady = client.queryConfigComplete(i);
                if (isReady) {
                    System.out.println("Server " + i + " is ready");
                } else {
                    isAllReady = false;
                    System.out.println("Server " + i + " is not ready");
                }
            }
            if (isAllReady) {
                break;
            }
            Thread.sleep(100);
        }
    }

    public static void doMigration(NovaClient client, List<String> servers, MigrationSummary summary, long thresholdForAtomicHandover) throws InterruptedException {
        System.out.println("Starting init phase");
        executeInitPhase(client, servers.size());
        System.out.println("Done init phase");
        System.out.println("Starting intermediate phase");
        executeIntermediatePhase(client, summary, thresholdForAtomicHandover);
        System.out.println("Done intermediate phase");
        executeConcludePhase(client, servers.size());
        System.out.println("Done conclude phase");
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        List<String> servers = Lists.newArrayList(args[0].split(","));
        String configFile = args[1];
        long thresholdForAtomicHandover = Long.parseLong(args[2]);

        System.out.println("Config file path: " + configFile);
        Configurations configs = ConfigurationUtil.readConfig(configFile);
        NovaClient client = new NovaClient(servers, true);

        int now = 0;
        for (int cfgId = 1; cfgId < configs.configs.size(); cfgId++) {
            long startTime = configs.configs.get(cfgId).startTimeInSeconds;
            while (true) {
                if (now == startTime) {
                    break;
                }
                System.out.println("Current iteration: " + now);
                Thread.sleep(1000);
                now++;
            }

            int cnt = 0;
            int max = 30;
            MigrationSummary summary = getMigrationSummary(configs, cfgId - 1, cfgId);

            long start = System.currentTimeMillis();
            doMigration(client, servers, summary, thresholdForAtomicHandover);
            long end = System.currentTimeMillis();

            System.out.println("It takes " + (end - start) + " to finish source driven migration of "
                    + summary.getFragmentsToSrcDst().size() + " fragments");
        }
    }
}
