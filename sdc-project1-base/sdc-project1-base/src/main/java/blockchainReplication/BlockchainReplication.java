package blockchainReplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import PBFT.PBFTAgreement;
import PBFT.notifications.DeliverEntryNotification;
import PBFT.notifications.NewViewNotification;
import PBFT.requests.AppendRequest;
import app.messages.client.LogEntry;
import blockchainBRB.notifications.DeliverBroadcastedEntryNotification;
import blockchainBRB.requests.BroadcastRequest;
import blockchainReplication.timers.ViewChangeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class BlockchainReplication extends GenericProtocol {

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "SMReplication";

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
        isMining = false;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //this.agreement = new PBFTAgreement();

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::onDeliverEntry);

        subscribeNotification(NewViewNotification.NOTIFICATION_ID, this::onNewView);

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




    private void onDeliverEntry(DeliverBroadcastedEntryNotification notification, short sourceProto) {

        try {

            Block block = Block.deserialize(notification.getClientRequest());

            if(isMining){
                currentMining.pauseMining();
                //Validar o bloco novo e ver se faz sentido adicionar, se sim, parar a mineração, se não, continuar
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

    private void onNewView(NewViewNotification notification, short sourceProto) {

    }

}
