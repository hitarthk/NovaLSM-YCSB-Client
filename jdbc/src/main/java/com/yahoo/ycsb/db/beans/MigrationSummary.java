package com.yahoo.ycsb.db.beans;

import java.util.List;
import java.util.Map;

public class MigrationSummary {
    private final Map<Integer, List<Long>> destinationToFragments;
    private final Map<Integer, List<Long>> sourceToFragments;
    private final Map<Integer, List<Integer>> fragmentsToSrcDst;


    public MigrationSummary(Map<Integer, List<Long>> destinationToFragments, Map<Integer, List<Long>> sourceToFragments, Map<Integer, List<Integer>> fragmentsToSrcDst) {
        this.destinationToFragments = destinationToFragments;
        this.sourceToFragments = sourceToFragments;
        this.fragmentsToSrcDst = fragmentsToSrcDst;
    }

    public Map<Integer, List<Long>> getDestinationToFragments() {
        return destinationToFragments;
    }

    public Map<Integer, List<Long>> getSourceToFragments() {
        return sourceToFragments;
    }

    public Map<Integer, List<Integer>> getFragmentsToSrcDst() {
        return fragmentsToSrcDst;
    }

    @Override
    public String toString() {
        return "MigrationSummary{" +
                "destinationToFragments=" + destinationToFragments +
                ", sourceToFragments=" + sourceToFragments +
                ", fragmentsToSrcDst=" + fragmentsToSrcDst +
                '}';
    }
}
