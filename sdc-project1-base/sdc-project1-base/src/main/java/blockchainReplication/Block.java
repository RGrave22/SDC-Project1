package blockchainReplication;

import PBFT.requests.AppendRequest;
import app.messages.client.LogEntry;
import io.netty.util.internal.StringUtil;
import utils.Crypto;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

class Block {
    private final String previousHash;
    private final List<AppendRequest> entries;
    private final long timestamp;
    private String hash;
    private int nonce;

    public Block(String previousHash, List<AppendRequest> entries) {
        this.previousHash = previousHash;
        this.entries = new ArrayList<>(entries);
        this.timestamp = System.currentTimeMillis();
        this.hash = calculateHash();
    }


    public String calculateHash() {
        String dataToHash = previousHash + Long.toString(timestamp) + Integer.toString(nonce) + entries.toString();
        return Crypto.applySha256(dataToHash);
    }

    public void mineBlock(int difficulty) {
        // Create a string with 'difficulty' number of zeros (target)
        String target = new String(new char[difficulty]).replace('\0', '0');

        // Increment nonce until the hash meets the difficulty target
        while (!hash.substring(0, difficulty).equals(target)) {
            nonce++;
            hash = calculateHash();  // Recalculate hash with the incremented nonce
        }
        System.out.println("Block mined! Hash: " + hash);
    }


    public String getPreviousHash() {
        return previousHash;
    }

    public List<LogEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    public String getHash() {
        return hash;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
