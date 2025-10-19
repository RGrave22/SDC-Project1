package simplereplication;

import broadcast.messages.BroadcastMessage;
import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;
import byzantinereliablebroadcast.notifications.DeliverEntryNotification;
import simplereplication.requests.AppendRequest;


import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

public class SimpleReplicationProtocol extends GenericProtocol {


    public final static short PROTO_ID = 900;
    public final static String PROTO_NAME = "SimpleReplication";

    private HashSet<UUID> delivered;
    private HashSet<Host> neighbors;
    private HashMap<Host, PublicKey> publicKeys;

    private Host mySelf;
    private PublicKey myPublicKey;
    private PrivateKey myPrivateKey;

    private int channelID;

    private static final Logger logger = LogManager.getLogger(SimpleReplicationProtocol.class);

    private final short underlyingProtocolID = 1000;

    public SimpleReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);

        delivered = new HashSet<UUID>();
        neighbors = new HashSet<Host>();
        publicKeys = new HashMap<Host, PublicKey>();

        mySelf = null;
        this.myPublicKey = null;
        this.myPrivateKey = null;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {


        subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);


        //lidar com o request do DLogManager
        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);

        //subscreve as notificações do BRB
        subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::handleDeliverNotification);

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

    private void uponNeighborUpNotification(NeighborUp notification, short i) {
        logger.info("Received NeighborUp notification for: " + notification.getNeighbor());
        this.neighbors.add(notification.getNeighbor());
        this.publicKeys.put(notification.getNeighbor(), notification.getPublicKey());
    }


    public void handleAppendRequest(AppendRequest request, short protoID) {
        logger.info("Received AppendRequest: UUID={} from client={}",
                request.getRequestID(), request.getClientID());

        sendRequest(request, underlyingProtocolID);

        logger.debug("Forwarded AppendRequest UUID={} to underlying protocol {}", request.getRequestID(), underlyingProtocolID);
    }

    public void handleDeliverNotification(DeliverEntryNotification notification, short sourceProto) {
//        logger.info("Delivered entry via BRB: UUID={} from sender={}",
//                notification.getRequestID(),
//                notification.getClientID());

        triggerNotification(notification);
    }



}
