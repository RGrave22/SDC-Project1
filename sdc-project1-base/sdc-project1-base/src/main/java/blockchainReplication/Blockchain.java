package blockchainReplication;

import PBFT.requests.AppendRequest;
import app.messages.client.LogEntry;

import java.util.ArrayList;
import java.util.List;


public class Blockchain {

    private final List<Block> blockchain = new ArrayList<>();

    //Difficulty é o numero de 0s no hash
    private final int difficulty;

    public Blockchain(int difficulty) {
        this.difficulty = difficulty;

        // Genesis block
        List<AppendRequest> logEntries = new ArrayList<>();
        String hash = "0";
        Block genesisBlock = new Block(hash, logEntries);
        genesisBlock.mineBlock(
                4,
                () -> {
                    blockchain.add(genesisBlock);
                    System.out.println("Genesis block added: " + genesisBlock.getHash());
                },
                () -> {
                    System.out.println("Mineration was canceled!");
                    //logica para quando a mineração é cancelada (normalmente quando um bloco novo chega)
                }
        );

    }

//    public void addBlock(Block newBlock) {
//
//        newBlock.mineBlock(difficulty);
//        blockchain.add(newBlock);
//    }

    public synchronized boolean addBlock(Block newBlock) {
        Block lastBlock = getLastBlock();

        //É suposto validar o bloco aqui?
        if (!validateBlock(newBlock)) {
            System.out.println("Block invalid");
            return false;
        }

        newBlock.mineBlock(
                4,
                () -> {
                    blockchain.add(newBlock);
                },
                () -> {
                    System.out.println("Mineration was canceled!");
                    //logica para quando a mineração é cancelada (normalmente quando um bloco novo chega)
                }
        );

        System.out.println("Block added to blockchain: " + newBlock.getHash());
        return true;
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

    public boolean validateBlock(Block block) {
        return block.getHash().equals(block.calculateHash());
    }

    public synchronized Block getLastBlock() {
        return blockchain.get(blockchain.size() - 1);
    }

    public synchronized List<Block> getStableBlocks() {
        if (blockchain.size() <= difficulty) {
            return new ArrayList<>();
        }
        return new ArrayList<>(blockchain.subList(0, blockchain.size() - difficulty));
    }

}