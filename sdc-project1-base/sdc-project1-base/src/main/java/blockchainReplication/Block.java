package blockchainReplication;

import PBFT.requests.AppendRequest;
import app.messages.client.LogEntry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.StringUtil;
import utils.Crypto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class Block {
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
        this.nonce = 0;
    }

    public Block(String previousHash, List<AppendRequest> entries, long timestamp, int nonce) {
        this.previousHash = previousHash;
        this.entries = new ArrayList<>(entries);
        this.timestamp = timestamp;
        this.hash = calculateHash();
        this.nonce = nonce;
    }


    public String calculateHash() {
        String dataToHash = previousHash + Long.toString(timestamp) + Integer.toString(nonce) + entries.toString();
        return Crypto.applySha256(dataToHash);
    }

    public void mineBlock(int difficulty) {
        if (difficulty < 0) throw new IllegalArgumentException("difficulty must be >= 0");
        final String targetPrefix = "0".repeat(difficulty);
        // Garante que o hash está alinhado com o nonce atual
        this.hash = calculateHash();
        while (!this.hash.startsWith(targetPrefix)) {
            this.nonce++;
            this.hash = calculateHash();
        }
        System.out.println("Block mined! Hash: " + this.hash);

    }

    public byte[] serialize() throws IOException {
        ByteBuf buf = Unpooled.buffer();

        byte[] prev = previousHash.getBytes();

        List<byte[]> blobs = new ArrayList<>(entries.size());
        for (AppendRequest ar : entries) {
            blobs.add(AppendRequest.serialize2(ar,buf));
        }

        buf.writeInt(prev.length);
        buf.writeBytes(prev);
        buf.writeLong(timestamp);
        buf.writeInt(nonce);
        buf.writeInt(entries.size());
        for (byte[] eb : blobs) {
            buf.writeInt(eb.length);
            buf.writeBytes(eb);
        }

        byte[] out = new byte[buf.readableBytes()];
        buf.getBytes(0, out);
        return out;
    }

    public static Block deserialize(byte[] data) throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(data);

        int version = buf.readInt();
        if (version != 1) throw new IllegalArgumentException("Unsupported version: " + version);

        int prevLen = buf.readInt();
        byte[] prev = new byte[prevLen];
        buf.readBytes(prev);
        String previousHash = new String(prev, StandardCharsets.UTF_8);

        long timestamp = buf.readLong();
        int nonce = buf.readInt();

        int n = buf.readInt();
        List<AppendRequest> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int len = buf.readInt();
            byte[] eb = new byte[len];
            buf.readBytes(eb);
            list.add(AppendRequest.deserialize(buf));
        }

        return new Block(previousHash, list, timestamp, nonce);
    }

    public String getPreviousHash() {
        return previousHash;
    }

    public List<AppendRequest> getEntries() {
        return entries;
    }

    public String getHash() {
        return hash;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
