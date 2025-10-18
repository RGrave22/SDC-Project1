package SMR;

import java.io.IOException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import PBFT.PBFTAgreement;
import PBFT.notifications.DeliverEntryNotification;
import PBFT.requests.AppendRequest;
import SMR.timers.ViewChangeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.handlers.MessageInHandler;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Crypto;

public class SMReplicationProtocol extends GenericProtocol {

    public static final short PROTO_ID = 1000;
    public static final String PROTO_NAME = "PBFTReplication";

    private PBFTAgreement agreement;

    public SMReplicationProtocol() {
        super(PROTO_NAME, PROTO_ID);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        this.agreement = new PBFTAgreement(this);

        List<Host> replicas = (List<Host>) props.get("replicas");
        Host self = (Host) props.get("self");
        Host primary = (Host) props.get("primary");

        String me = props.getProperty("replica_name");
        PrivateKey privKey;
        try {
            privKey = Crypto.getPrivateKey(me, props);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao carregar chave privada da réplica " + me, e);
        }

        Map<Host, PublicKey> publicKeys = new HashMap<>();
        try {
            KeyStore truststore = Crypto.getTruststore(props);
            for (Host h : replicas) {
                java.security.cert.Certificate cert = truststore.getCertificate(h.getAddress().toString());
                if (cert != null) {
                    publicKeys.put(h, cert.getPublicKey());
                } else {
                    throw new RuntimeException("Certificado não encontrado para " + h.getAddress().toString());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Erro ao carregar chaves públicas dos replicas", e);
        }

        agreement.init(replicas, self, primary, privKey, publicKeys);

        registerRequestHandler(AppendRequest.REQUEST_ID, this::onAppend);

        setupPeriodicTimer(new ViewChangeTimer(), 3000, 3000);

        registerTimerHandler(ViewChangeTimer.TIMER_ID, (ViewChangeTimer t) -> {
            agreement.checkPrimaryTimeout();
        });

    }

    private void onAppend(AppendRequest req, short fromProto) {
        agreement.propose(req);
    }

    public void sendToReplica(ProtoMessage msg, Host h) {
        sendMessage(msg, h);
    }

    public void deliverEntry(AppendRequest req, long seqNo) {

        triggerNotification(new DeliverEntryNotification(
                req.getRequestID(),
                req.getClientID(),
                req.getClientRequest(),
                req.getServerSignature(),
                seqNo
        ));

    }

    public <T extends ProtoMessage> void registerPBFTHandler(short msgId, MessageInHandler<T> handler) throws HandlerRegistrationException {
        registerMessageHandler(PROTO_ID, msgId, handler);
    }

}
