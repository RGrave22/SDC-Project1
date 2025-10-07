package simplereplication;

import common.events.ChannelAvailable;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;
import byzantinereliablebroadcast.notifications.DeliverEntryNotification;
import replication.requests.AppendRequest;
import simplereplication.messages.AppendMessage;

import java.io.IOException;
import java.util.Properties;

public class SimpleReplicationProtocol extends GenericProtocol {


    public final static short PROTO_ID = 900;
    public final static String PROTO_NAME = "SimpleReplication";

    private Host mySelf;

    private final short underlyingProtocolID = 1000;

    public SimpleReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);

        mySelf = null;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //lidar com o request do DLogManager
        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);

        //subscreve as notificações do BRB
        subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::handleDeliverNotification);
    }

    //Nao sei se é suposto ser so isto, pelo enunciado parece que sim, mas por outro lado não parece ser assim tao simples.
    public void handleAppendRequest(AppendRequest request, short protoID) {

        sendRequest(request, underlyingProtocolID);

    }


    public void handleDeliverNotification(DeliverEntryNotification notification, short sourceProto) {

        triggerNotification(notification);

    }




}
