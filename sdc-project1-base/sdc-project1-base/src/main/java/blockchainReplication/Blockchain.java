package blockchainReplication;

import java.util.ArrayList;
import java.util.List;

import blockchainBRB.requests.AppendRequest;

public class Blockchain {

    private final List<Block> blockchain = new ArrayList<>();

    //Difficulty é o numero de 0s no hash
    private final int difficulty;

    public Blockchain(int difficulty) {
        this.difficulty = difficulty;

        // Genesis block
        List<AppendRequest> logEntries = new ArrayList<>();
        String hash = "0";
        long timestamp = 0L; // Timestamp fixo
        int genesisNonce = 111111;
        Block genesisBlock = new Block(hash, logEntries, timestamp, genesisNonce);

        blockchain.add(genesisBlock);
        System.out.println("Genesis block added: " + genesisBlock.getHash());
    }

//    public void addBlock(Block newBlock) {
//
//        newBlock.mineBlock(difficulty);
//        blockchain.add(newBlock);
//    }
    //add a new block (received from other node/miner)
    public void addMinedBlock(Block newBlock) {
        blockchain.add(newBlock);
    }

    //get a block that has already been confirmed in the chain (according to the value determined in the replication protocol)
    public Block getNewValidBlock(int positionBehind) {
        return blockchain.size() > positionBehind + 1 ? blockchain.get(blockchain.size() - 1 - positionBehind) : null;
    }

    public boolean isChainValid() {
        Block currentBlock;
        Block previousBlock;

        for (int i = 1; i < blockchain.size(); i++) {
            currentBlock = blockchain.get(i);
            previousBlock = blockchain.get(i - 1);

            if (!currentBlock.getHash().equals(currentBlock.calculateHash())) {
                System.out.println("Current block hash is invalid.");
                return false;
            }

            if (!previousBlock.getHash().equals(currentBlock.getPreviousHash())) {
                System.out.println("Previous block hash is invalid.");
                return false;
            }
        }
        return true;
    }

    public List<Block> getBlockchain() {
        return blockchain;
    }

    public void displayBlockchain() {
        for (Block block : blockchain) {
            System.out.println(block.toString());
        }
    }

    public boolean isForkInit(Block block) {
        return blockchain.size() > 1 ? block.getPreviousHash().equals(blockchain.get(blockchain.size() - 2).getHash()) : false;
    }

    public boolean alreadyLastBlock(Block block) {
        return block.getHash().equals(blockchain.get(blockchain.size() - 1).getHash());
    }

    public boolean validateBlock(Block block) {
        //validate block hash
        if (!block.getHash().substring(0, difficulty).equals("0".repeat(difficulty))) {
            System.out.println("Malicious block: " + block.getHash());
            return false;
        }
        //validate previous hash
        if (!block.getPreviousHash().equals(getLastBlock().getHash())) {
            System.out.println("Fork: " + block.getHash());
            System.out.println("Previous hash: " + block.getPreviousHash());
            System.out.println("Last Block hash: " + this.getLastBlock().getHash());
            System.out.println("Block chain: " + this.blockchain);
            return false;
        }
        boolean isValidHash = block.getHash().equals(block.calculateHash());
        System.out.println("Estamos a verificar se o hash está bem feito: " + block.getHash() + ": " + block.calculateHash());
        return isValidHash;
    }


    public void removeNotValid(String hash){
        while(blockchain.getLast().getPreviousHash().equals(hash)){
            blockchain.remove(blockchain.size() - 1);
        }
    }


    public Block getLastBlock() {
        return blockchain.get(blockchain.size() - 1);
    }

    public synchronized List<Block> getStableBlocks() {
        if (blockchain.size() <= difficulty) {
            return new ArrayList<>();
        }
        return new ArrayList<>(blockchain.subList(0, blockchain.size() - difficulty));
    }

}
