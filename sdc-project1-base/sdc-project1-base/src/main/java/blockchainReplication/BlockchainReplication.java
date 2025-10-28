package blockchainReplication;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;

import PBFT.PBFTAgreement;
import PBFT.notifications.DeliverEntryNotification;
import PBFT.notifications.NewViewNotification;
import PBFT.requests.AppendRequest;
import app.messages.client.LogEntry;
import blockchainBRB.ByzantineReliableBroadcastProtocol;
import blockchainBRB.notifications.DeliverBroadcastedEntryNotification;
import blockchainBRB.requests.BroadcastRequest;
import blockchainReplication.timers.ViewChangeTimer;
import common.events.NeighborDown;
import common.events.NeighborUp;
import common.events.ChannelAvailable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class BlockchainReplication extends GenericProtocol {

    private final Logger logger = LogManager.getLogger(BlockchainReplication.class);

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "BlockchainReplication";

    private HashSet<Host> neighbors;
    private HashMap<Host, PublicKey> publicKeys;

    private Host mySelf;
    private PublicKey myPublicKey;
    private PrivateKey myPrivateKey;

    private int channelID;


    private PBFTAgreement agreement;
    private final Map<Long, AppendRequest> log;
    private final Map<UUID, AppendRequest> pendingRequests;

    int numberOfRequestsPerBlock = 5;
    int difficulty = 4; //Podemos mudar a dificuldade
    private Blockchain blockchain = new Blockchain(difficulty);
    private boolean isMining;
    private Block currentMining;



    public BlockchainReplication() {
        super(PROTO_NAME, PROTO_ID);
        this.log = new TreeMap<>();
        this.pendingRequests = new HashMap<>();

        neighbors = new HashSet<Host>();
        publicKeys = new HashMap<Host, PublicKey>();

        mySelf = null;
        this.myPublicKey = null;
        this.myPrivateKey = null;

        isMining = false;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //this.agreement = new PBFTAgreement();

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::onDeliverEntry);

        subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);

//        subscribeNotification(NewViewNotification.NOTIFICATION_ID, this::onNewView);

//        setupPeriodicTimer(new ViewChangeTimer(), 3000, 3000);
//
//        registerTimerHandler(ViewChangeTimer.TIMER_ID, (ViewChangeTimer t, 3000) -> {
//            agreement.checkPrimaryTimeout();
//        });



    }

    private void onAppend(AppendRequest req, short fromProto) {
        pendingRequests.put(req.getRequestID(), req);
        //agreement.propose(req);
        if(pendingRequests.size() >= numberOfRequestsPerBlock && !isMining) {
            createBlock();
            isMining = true;
        }else{
            System.out.println("Node mining...");
        }

    }

    private void createBlock() {
        List<AppendRequest> toInclude = new ArrayList<>(pendingRequests.values());

        Block lastBlock = blockchain.getLastBlock();
        Block newBlock = new Block(lastBlock.getHash(), toInclude);

        //Minerar o bloco
        if(blockchain.addBlock(newBlock)){
            currentMining =  newBlock;
        }


    }

    protected void broadCastBlock(Block block) {
        try{
            isMining = false;
            AppendRequest lastRequest = block.getEntries().getLast();
            BroadcastRequest broadcastRequest = new BroadcastRequest(lastRequest.getRequestID().toString(), lastRequest.getClientID(), lastRequest.getSource(), block.serialize());

        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    private void sendBlock() {
        try {



            List<AppendRequest> toInclude = new ArrayList<>(pendingRequests.values());
            //pendingRequests.clear();

            Block lastBlock = blockchain.getLastBlock();
            Block newBlock = new Block(lastBlock.getHash(), toInclude);

            //start mining the new block
            newBlock.mineBlock(difficulty);


            //if (accepted) {
                //Não sei bem como colocar o id, mas pronto (pus tudo do ultimo pedido da lista do bloco)
            AppendRequest lastRequest = newBlock.getEntries().getLast();
            BroadcastRequest broadcastRequest = new BroadcastRequest(lastRequest.getRequestID().toString(), lastRequest.getClientID(), lastRequest.getSource(), newBlock.serialize());
            //}

        }catch (IOException e){
            System.out.println("Error: " + e.getMessage());
        }
    }



    private void onDeliverEntry(DeliverBroadcastedEntryNotification notification, short sourceProto) {

        try {

            Block block = Block.deserialize(notification.getClientRequest());

            if(blockchain.validateBlock(block)){

            }

//            boolean validate = blockchain.addBlock(block);
//
//            if(validate) {
//                List<AppendRequest> toRemove = block.getEntries();
//                for(AppendRequest appendRequest : toRemove) {
//                    pendingRequests.remove(appendRequest.getRequestID());
//
//                    //wait(1000);
//                    //Acho que esta mal criada
//                    DeliverEntryNotification deliverEntryNotification = new DeliverEntryNotification(appendRequest.getRequestID(), appendRequest.getClientID(), appendRequest.getClientRequest(), null, 0);
//                    triggerNotification(deliverEntryNotification);
//                }
//
//
//            }



        }catch (IOException e){
            System.out.println("Error: " + e.getMessage());
        }

    }

    public List<AppendRequest> getStateMachineLog() {
        return new ArrayList<>(log.values());
    }

//    private void onNewView(NewViewNotification notification, short sourceProto) {
//    }



    private void handleChannelAvailableNotification(ChannelAvailable notification, short i) {
        this.mySelf = notification.getMyHost();
        this.myPublicKey = notification.getMyPublicKey();
        this.myPrivateKey = notification.getMyPrivateKey();

        this.publicKeys.put(mySelf, myPublicKey);

        this.channelID = notification.getChannelID();

        registerSharedChannel(this.channelID);
        setDefaultChannel(this.channelID);
    private void onNewView(NewViewNotification notification, short sourceProto) {

    }

    private void uponNeighborDownNotification(NeighborDown notification, short i) {
        logger.info("Received NeighborDown notification for: " + notification.getNeighbor());
        this.neighbors.remove(notification.getNeighbor());
    }

    private void uponNeighborUpNotification(NeighborUp notification, short i) {
        logger.info("Received NeighborUp notification for: " + notification.getNeighbor());
        this.neighbors.add(notification.getNeighbor());
        this.publicKeys.put(notification.getNeighbor(), notification.getPublicKey());
    }


}
