package com.yahoo.ycsb.db.beans;

public class LogReplaySummary {
    private long iteration;
    private long bytesReplayed;
    private long recordsReplayed;


    public LogReplaySummary(long iteration, long bytesReplayed, long recordsReplayed) {
        this.iteration = iteration;
        this.bytesReplayed = bytesReplayed;
        this.recordsReplayed = recordsReplayed;
    }

    public LogReplaySummary() {
        this.iteration = 0;
        this.bytesReplayed = 0;
        this.recordsReplayed = 0;
    }

    public long getIteration() {
        return iteration;
    }

    public long getBytesReplayed() {
        return bytesReplayed;
    }

    public long getRecordsReplayed() {
        return recordsReplayed;
    }

    public void setIteration(long iteration) {
        this.iteration = iteration;
    }

    public void setBytesReplayed(long bytesReplayed) {
        this.bytesReplayed = bytesReplayed;
    }

    public void setRecordsReplayed(long recordsReplayed) {
        this.recordsReplayed = recordsReplayed;
    }

    public static LogReplaySummary parse(String s) {
        String[] split = s.split("\\|");
        long iteration = Long.parseLong(split[0]);
        long recordsReplayed = Long.parseLong(split[1]);
        long bytesReplayed = Long.parseLong(split[2]);
        return new LogReplaySummary(iteration, bytesReplayed, recordsReplayed);
    }

    @Override
    public String toString() {
        return "LogReplaySummary{" +
                "iteration=" + iteration +
                ", bytesReplayed=" + bytesReplayed +
                ", recordsReplayed=" + recordsReplayed +
                '}';
    }
}
