package PBFT;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import PBFT.requests.AppendRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class PBFTLogEntry {

    private final long seq;
    private final long view;
    private final AppendRequest request;
    private final byte[] digest;

    private final Set<Host> prepares;
    private final Set<Host> commits;
    private boolean delivered = false;

    public PBFTLogEntry(long seq, long view, AppendRequest request, byte[] digest) {
        this.seq = seq;
        this.view = view;
        this.request = request;
        this.prepares = new HashSet<>();
        this.commits = new HashSet<>();
        this.digest = digest;
    }

    public long getSeq() {
        return seq;
    }

    public long getView() {
        return view;
    }

    public AppendRequest getRequest() {
        return request;
    }

    public byte[] getDigest() {
        return digest;
    }

    public void addPrepare(Host h) {
        prepares.add(h);
    }

    public int getPreparesCount() {
        return prepares.size();
    }

    public void addCommit(Host h) {
        commits.add(h);
    }

    public int getCommitsCount() {
        return commits.size();
    }

    public boolean isDelivered() {
        return delivered;
    }

    public void markDelivered() {
        this.delivered = true;
    }

    public boolean matchesDigest(byte[] otherDigest) {
        return Arrays.equals(digest, otherDigest);
    }

    @Override
    public String toString() {
        return "PBFTLogEntry (seq=" + seq + ", prepares=" + prepares.size() + ", commits=" + commits.size() + ")";
    }

    public boolean hasCommitFrom(Host h) {
        return commits.contains(h);
    }

}
