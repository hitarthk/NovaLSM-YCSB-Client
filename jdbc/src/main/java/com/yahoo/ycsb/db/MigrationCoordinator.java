package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.db.ConfigurationUtil.Configurations;
import com.yahoo.ycsb.db.ConfigurationUtil.Configuration;

public class MigrationCoordinator {

    // These should exactly match the contents in NovaGlobalVariables in nova_common.h
    static char MIGRATION_INIT_PHASE = 'F';
    static char MIGRATION_INTERMEDIATE_PHASE = 'S';
    static char MIGRATION_CONCLUDE_PHASE = 'L';

    // todo: add functionality for getting map of migrating ranges from source
    private static Map<Integer, List<Long>> getAllDestinationsDuringMigration(Configurations configs, int currentConfigId, int newConfigId) {
        Map<Integer, List<Long>> migratedFragmentsMap = new HashMap<>();
        Configuration currentConfig = configs.configs.get(currentConfigId);
        Configuration newConfig = configs.configs.get(newConfigId);
        assert currentConfig.fragments.size() == newConfig.fragments.size();
        for (int i = 0; i < currentConfig.fragments.size(); i++) {
            LTCFragment currentFrag = currentConfig.fragments.get(i);
            LTCFragment newFrag = newConfig.fragments.get(i);
            System.out.println("Fragment ids: " + currentFrag.dbId + "|" + newFrag.dbId + " ltcServer ids: " + currentFrag.ltcServerId + "|" + newFrag.ltcServerId);
            if (currentFrag.ltcServerId != newFrag.ltcServerId) {
                List<Long> migratedFragments = migratedFragmentsMap.getOrDefault(newFrag.ltcServerId, new ArrayList<Long>());
                migratedFragments.add((long) i);
                migratedFragmentsMap.put(newFrag.ltcServerId, migratedFragments);
            }
        }
        System.out.println("Migrated fragments map: " + migratedFragmentsMap);
        return migratedFragmentsMap;
    }

    public static void main(String[] args) throws Exception {
        List<String> servers = Lists.newArrayList(args[0].split(","));
        String configFile = args[1];
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

            for (int i = 0; i <servers.size(); i++) {
                System.out.println("Initiating migration in node: " + i);
                client.initiateSourceDrivenMigration(i, MIGRATION_INIT_PHASE);
                System.out.println("Src driven migration initiated on node: " + i);
            }

            long start = System.currentTimeMillis();
//            while (true) {
//                boolean isAllReady = true;
//                for (int i = 0; i < servers.size(); i++) {
//                    boolean isReady = client.queryConfigComplete(i);
//                    if (isReady) {
//                        System.out.println("Server " + i + " is ready");
//                    } else {
//                        isAllReady = false;
//                        System.out.println("Server " + i + " is not ready");
//                    }
//                }
//                if (isAllReady) {
//                    break;
//                }
//                Thread.sleep(100);
//            }

            int cnt = 0;
            int max = 30;
            Map<Integer, List<Long>> migartedFragmentsMap = getAllDestinationsDuringMigration(configs, cfgId - 1, cfgId);

            while (cnt < max) {
                System.out.println("Sending request");
                for (Map.Entry<Integer, List<Long>> e : migartedFragmentsMap.entrySet()) {
                    Integer serverId = e.getKey();
                    List<Long> migratedFragments = e.getValue();
                    for (Long fragId : migratedFragments) {
                        System.out.println("Querying " + serverId + " for range " + fragId);
                        NovaClient.ReturnValue v = client.getLogRecordsReplayed(serverId, fragId);
                        System.out.println("Found value: " + v.getValue);
                    }
                }
                Thread.sleep(1000);
                cnt++;
            }

            long end = System.currentTimeMillis();
            long duration = end - start;
            System.out.println(cfgId + " Take to complete configuration change " + duration);
        }
    }
}
