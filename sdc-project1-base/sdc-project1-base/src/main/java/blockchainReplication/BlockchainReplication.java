package blockchainReplication;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;

import PBFT.PBFTAgreement;
import PBFT.notifications.DeliverEntryNotification;
import blockchainBRB.requests.AppendRequest;
import blockchainBRB.ByzantineReliableBroadcastProtocol;
import blockchainBRB.messages.BlockchainReplyMessage;
import blockchainBRB.messages.BlockchainRequestMessage;
import blockchainBRB.notifications.DeliverBroadcastedEntryNotification;
import blockchainBRB.requests.BroadcastRequest;
import common.events.NeighborDown;
import common.events.NeighborUp;
import common.events.ChannelAvailable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class BlockchainReplication extends GenericProtocol {

    private final Logger logger = LogManager.getLogger(BlockchainReplication.class);

    public static final short PROTO_ID = 600;
    public static final String PROTO_NAME = "BlockchainReplication";
    public static final short underlyingLayer = 1000;

    private HashSet<Host> neighbors;
    private HashMap<Host, PublicKey> publicKeys;

    private Host mySelf;
    private PublicKey myPublicKey;
    private PrivateKey myPrivateKey;

    private int channelID;

    private PBFTAgreement agreement;
    private final Map<Long, AppendRequest> log;
    private final Queue<AppendRequest> pendingRequests;
    private final Map<String, List<Block>> subchains;
    private final List<AppendRequest> requestsBeingUsed;

    private final int numberOfRequestsPerBlock = 5;
    private final int difficulty = 5; //Podemos mudar a dificuldade
    private final Blockchain blockchain;
    private boolean isMining;
    private Block currentMining;

    private static final int CONFIRMATION = 3;

    public BlockchainReplication() {
        super(PROTO_NAME, PROTO_ID);
        this.log = new TreeMap<>();
        this.pendingRequests = new LinkedList<>();

        this.blockchain = new Blockchain(difficulty);

        neighbors = new HashSet<>();
        publicKeys = new HashMap<>();

        mySelf = null;
        this.myPublicKey = null;
        this.myPrivateKey = null;

        isMining = false;
        subchains = new HashMap<>();
        requestsBeingUsed = new ArrayList<>();
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //this.agreement = new PBFTAgreement();
        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        subscribeNotification(DeliverBroadcastedEntryNotification.NOTIFICATION_ID, this::onBroadcastedBlockNotification);

        subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);

//        registerMessageHandler(channelID, BlockchainReplyMessage.MESSAGE_ID, this::handleBlockchainReply);
//        registerMessageHandler(channelID, BlockchainRequestMessage.MESSAGE_ID, this::handleRequestBlockchain);
//        BlockchainRequestMessage requestBlockchainMsg = new BlockchainRequestMessage(mySelf);
//        while(blockchain.getBlockchain().isEmpty()){
//            for (Host h : neighbors) {
//                if (!h.equals(mySelf)) {
//                    System.out.println("Requested Blockchain to neighbor " + h.getAddress());
//                    sendMessage(requestBlockchainMsg, h);
//                }
//            }
//        }
    }

    private void onAppend(AppendRequest req, short fromProto) {
        pendingRequests.add(req);

        System.out.println("Received request: " + req.getRequestID());

        //agreement.propose(req);
        if (pendingRequests.size() >= numberOfRequestsPerBlock) {

            if (!isMining) {
                createBlock();
                isMining = true;
            } else {
                System.out.println("Node mining...");
            }
        } else {
            System.out.println("Not enough requests to mine yet...");
        }
    }

    //Receiving a request to send the blockchain to another node
    private void handleRequestBlockchain(ProtoMessage protoMessage, Host sender, short protoId, int channel) {
        BlockchainRequestMessage msg = (BlockchainRequestMessage) protoMessage;

        logger.info("Received blockchain request from " + sender.getAddress() + ": " + protoId);

        if (this.publicKeys.containsKey(msg.getSender()) && !sender.equals(mySelf)) {
            try {
                if (!msg.checkSignature(this.publicKeys.get(msg.getSender()))) {
                    throw new Exception();
                }
            } catch (Exception e) {
                logger.error("Could not verify authenticity of BroadcastMessage sent by " + msg.getSender());
                e.printStackTrace();
            }
        } else if (msg.getSender().equals(mySelf)) {
            logger.info("Message received from myself, no need to check the signature.");
        } else {
            logger.warn("I do not know the public key of " + msg.getSender() + " to validate the message.");
        }
    }

    //receiving a blockchain from another node and processing it
    private void handleBlockchainReply(ProtoMessage protoMessage, Host host, short i, int i1) {

    }

    private void createBlock() {
        List<AppendRequest> toInclude = new ArrayList<>();

        for (int i = 0; i < numberOfRequestsPerBlock; i++) {
            AppendRequest removed = pendingRequests.remove();
            toInclude.add(removed);
            requestsBeingUsed.add(removed);
        }

        Block lastBlock = blockchain.getLastBlock();
        Block newBlock = new Block(lastBlock.getHash(), toInclude);

        //Minerar o bloco
        currentMining = newBlock;
        addBlock(newBlock);
    }

    public void broadCastBlock(Block block) {
        try {
            isMining = false;
            AppendRequest lastRequest = block.getEntries().getLast();
            BroadcastRequest broadcastRequest = new BroadcastRequest(lastRequest.getRequestID().toString(), lastRequest.getClientID(), lastRequest.getSource(), block.serialize());

            sendRequest(broadcastRequest, underlyingLayer);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void sendBlock() {
        try {

            List<AppendRequest> toInclude = new ArrayList<>(pendingRequests);
            //pendingRequests.clear();

            Block lastBlock = blockchain.getLastBlock();
            Block newBlock = new Block(lastBlock.getHash(), toInclude);

            //start mining the new block
            //newBlock.mineBlock(difficulty);
            //if (accepted) {
            //Não sei bem como colocar o id, mas pronto (pus tudo do ultimo pedido da lista do bloco)
            AppendRequest lastRequest = newBlock.getEntries().getLast();
            BroadcastRequest broadcastRequest = new BroadcastRequest(lastRequest.getRequestID().toString(), lastRequest.getClientID(), lastRequest.getSource(), newBlock.serialize());
            //}

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private void onBroadcastedBlockNotification(DeliverBroadcastedEntryNotification notification, short sourceProto) {

        try {

            BroadcastRequest breq = BroadcastRequest.deserialize(notification.getClientRequest());

            Block block = Block.deserialize(breq.getClientRequest());

            System.out.println("Block check hash when received: " + block.getHash() + ": " + block.calculateHash());
            System.out.println("Block check all: " + block.toString());

            if (blockchain.alreadyLastBlock(block)) {
                logger.info("Block already at last place in the chain");
                Block newConfirmed = blockchain.getNewValidBlock(CONFIRMATION);

                if (newConfirmed != null) {

                    for (AppendRequest appendRequest : newConfirmed.getEntries()) {
                        //Faz mais sentido fazer aqui, ou enviar o bloco para o LogManager?
                        DeliverEntryNotification deliverEntryNotification = new DeliverEntryNotification(appendRequest.getRequestID(), appendRequest.getClientID(), appendRequest.getClientRequest(), null, 0);
                        triggerNotification(deliverEntryNotification);
                        System.out.println("Append Request delivered to DLM: " + deliverEntryNotification.getRequestID());
                        //wait(3000);
                    }

                    if (subchains.containsKey(newConfirmed.getPreviousHash())) {
                        List<Block> toRemove = subchains.get(newConfirmed.getPreviousHash());

                        for (Block b : toRemove) {
                            for (AppendRequest appendRequest : b.getEntries()) {
                                if (requestsBeingUsed.contains(appendRequest)) {
                                    pendingRequests.add(appendRequest);
                                    requestsBeingUsed.remove(appendRequest);
                                }
                            }
                            System.out.println("Chain confirmed, removing subchain: " + b.getHash());
                        }

                        subchains.remove(newConfirmed.getPreviousHash());

                    }
                }

            } else if (blockchain.validateBlock(block)) {

                if (isMining) {
                    currentMining.stopMining();

                    for (AppendRequest appendRequest : currentMining.getEntries()) {
                        if (requestsBeingUsed.contains(appendRequest)) {
                            pendingRequests.add(appendRequest);
                            requestsBeingUsed.remove(appendRequest);
                        }
                    }
                    currentMining = null;
                }
                blockchain.addMinedBlock(block);

                System.out.println("Block received from broadcast and added to the chain: " + block.getHash());

                Block newConfirmed = blockchain.getNewValidBlock(CONFIRMATION);

                if (newConfirmed != null) {

                    for (AppendRequest appendRequest : newConfirmed.getEntries()) {
                        //Faz mais sentido fazer aqui, ou enviar o bloco para o LogManager?
                        DeliverEntryNotification deliverEntryNotification = new DeliverEntryNotification(appendRequest.getRequestID(), appendRequest.getClientID(), appendRequest.getClientRequest(), null, 0);
                        triggerNotification(deliverEntryNotification);
                        System.out.println("Append Request delivered to DLM: " + deliverEntryNotification.getRequestID());
                        //wait(3000);
                    }

                    if (subchains.containsKey(newConfirmed.getPreviousHash())) {
                        List<Block> toRemove = subchains.get(newConfirmed.getPreviousHash());

                        for (Block b : toRemove) {
                            for (AppendRequest appendRequest : b.getEntries()) {
                                if (requestsBeingUsed.contains(appendRequest)) {
                                    pendingRequests.add(appendRequest);
                                    requestsBeingUsed.remove(appendRequest);
                                }
                            }
                            System.out.println("Chain confirmed, removing subchain: " + b.getHash());
                        }

                        subchains.remove(newConfirmed.getPreviousHash());

                    }
                }

            } else {
                checkFork(block);
            }

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }

    }

    public void checkFork(Block block) {
        if (blockchain.isForkInit(block)) {
            List<Block> newSubchain = new ArrayList<>();
            newSubchain.add(block);
            subchains.put(block.getPreviousHash(), newSubchain);
            System.out.println("Fork identified: " + block.getHash());
        } else {

            for (Map.Entry<String, List<Block>> entry : subchains.entrySet()) {
                List<Block> subchain = entry.getValue();

                if (block.getPreviousHash().equals(subchain.getLast().getHash())) {
                    subchain.add(block);
                    System.out.println("New block on subchain: " + block.getHash());

                    //TODO Verificar se é suposto ser esta condição
                    if (subchain.size() >= CONFIRMATION) {
                        System.out.println("Subchain confirmed!");
                        //Ver tambem se este not valid esta bom
                        blockchain.removeNotValid(entry.getKey());

                        for (Block b : subchain) {
                            blockchain.addMinedBlock(b);
                        }

                        subchains.remove(entry.getKey());

                        try {

                            Block newConfirmed = blockchain.getNewValidBlock(CONFIRMATION);

                            if (newConfirmed != null) {

                                for (AppendRequest appendRequest : newConfirmed.getEntries()) {
                                    //Faz mais sentido fazer aqui, ou enviar o bloco para o LogManager?
                                    DeliverEntryNotification deliverEntryNotification = new DeliverEntryNotification(appendRequest.getRequestID(), appendRequest.getClientID(), appendRequest.getClientRequest(), null, 0);
                                    triggerNotification(deliverEntryNotification);
                                    System.out.println("Append Request delivered to DLM: " + deliverEntryNotification.getRequestID());
                                    //wait(3000);
                                }

                            }
                        } catch (Exception e) {
                            System.out.println("Error: " + e.getMessage());
                        }

                    }
                    break;
                }
            }
        }
    }

    public synchronized boolean addBlock(Block newBlock) {

        newBlock.mineBlock(
                difficulty,
                () -> {

                    blockchain.addMinedBlock(newBlock);
                    System.out.println("Block added to blockchain: " + newBlock.getHash() + ": " + newBlock.calculateHash());
                    System.out.println("Block added: " + newBlock.toString());
                    broadCastBlock(newBlock);
                },
                () -> {
                    System.out.println("Mineration was canceled!");
                    isMining = false;
                    //logica para quando a mineração é cancelada (normalmente quando um bloco novo chega)
                }
        );

        return true;
    }

    public List<AppendRequest> getStateMachineLog() {
        return new ArrayList<>(log.values());
    }

    private void handleChannelAvailableNotification(ChannelAvailable notification, short i) {
        this.mySelf = notification.getMyHost();
        this.myPublicKey = notification.getMyPublicKey();
        this.myPrivateKey = notification.getMyPrivateKey();

        this.publicKeys.put(mySelf, myPublicKey);

        this.channelID = notification.getChannelID();

        registerSharedChannel(this.channelID);
        setDefaultChannel(this.channelID);

    }

    private void uponNeighborDownNotification(NeighborDown notification, short i) {
        logger.info("Received NeighborDown notification for: " + notification.getNeighbor());
        this.neighbors.remove(notification.getNeighbor());
    }

    public void uponNeighborUpNotification(NeighborUp notification, short i) {
        logger.info("Received NeighborUp notification for: " + notification.getNeighbor());
        this.neighbors.add(notification.getNeighbor());
        this.publicKeys.put(notification.getNeighbor(), notification.getPublicKey());
    }
}
