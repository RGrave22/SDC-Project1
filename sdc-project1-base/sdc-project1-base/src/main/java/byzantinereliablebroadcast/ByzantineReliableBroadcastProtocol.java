package byzantinereliablebroadcast;

import byzantinereliablebroadcast.messages.BRBEchoMessage;
import byzantinereliablebroadcast.messages.BRBReadyMessage;
import byzantinereliablebroadcast.messages.BRBSendMessage;
import membership.staticmembership.message.HelloMessage;
import membership.staticmembership.message.HelloReplyMessage;
import membership.staticmembership.message.ReplyMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
import simplereplication.requests.AppendRequest;
import utils.Crypto;

import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class ByzantineReliableBroadcastProtocol extends GenericProtocol {

    public final static String PAR_MYHOST = "brb.myhost";
    public final static String PAR_NEIGHBORS = "brb.neighbors";

    public final static short PROTO_ID = 1000;
    public final static String PROTO_NAME = "ByzantineReliableBroadcastProtocol";

    private HashMap<Host, PublicKey> neighbors;
    private HashSet<Host> candidates;
    private HashMap<Host, PublicKey> publicKeys;


    private Host myself;
    private int channelID;

    private PrivateKey privateKey;
    private KeyStore truststore;
    private PublicKey publicKey;
    private String myIdentifier;

    private HashMap<Host, Long> issuedChallenges;

    //Para guardar os ecos e as delivered
    private HashMap<String, HashSet<Host>> echoMsgs;
    private HashSet<String> delivered;

    public ByzantineReliableBroadcastProtocol() {
        super(PROTO_NAME, PROTO_ID);

        this.neighbors = new HashMap<Host, PublicKey>();
        this.publicKeys = new HashMap<Host, PublicKey>();


        this.issuedChallenges = new HashMap<Host, Long>();

        this.myself = null;

        this.privateKey = null;
        this.truststore = null;
        this.publicKey = null;

        this.myIdentifier = null;

        //Iniciar os ecos e delivered
        echoMsgs = new HashMap<>();
        delivered = new HashSet<>();

    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        if (!props.containsKey(PAR_MYHOST) || !props.containsKey(PAR_NEIGHBORS) || ! props.containsKey(Crypto.CRYPTO_NAME_KEY)) {
            if (!props.contains(PAR_MYHOST))
                System.err.println("Protocol " + PROTO_NAME + " requires mandatory parameter: " + PAR_MYHOST
                        + " (format: IP:PORT)");
            if (!props.containsKey(PAR_NEIGHBORS))
                System.err.println("Protocol " + PROTO_NAME + " requires mandatory parameter: " + PAR_NEIGHBORS
                        + " (format: IP:PORT,IP:PORT,...,IP:PORT");
            if(!props.containsKey(Crypto.CRYPTO_NAME_KEY)) {
                System.err.println("Protocol " + PROTO_NAME + " requires mandatory paramter " + Crypto.CRYPTO_NAME_KEY);
            }
            System.exit(1);
        }

        myIdentifier = props.getProperty(Crypto.CRYPTO_NAME_KEY);

        String[] hostElements = props.getProperty(PAR_MYHOST).split(":");

        this.myself = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));

        //criaçao de um canal tcp para comunicaçao entre todos os processos/neighbors
        Properties channelProps = new Properties();
        channelProps.put(TCPChannel.ADDRESS_KEY, this.myself.getAddress().toString().replace("/", ""));
        channelProps.put(TCPChannel.PORT_KEY, this.myself.getPort() + "");

        channelID = createChannel(TCPChannel.NAME, channelProps);


        //Nao sei se esta correto
        /*-------------------- Register Message Serializers ------------------------------- */
        registerMessageSerializer(channelID, BRBSendMessage.MESSAGE_ID, BRBSendMessage.serializer);
        registerMessageSerializer(channelID, BRBEchoMessage.MESSAGE_ID, BRBEchoMessage.serializer);
        registerMessageSerializer(channelID, BRBReadyMessage.MESSAGE_ID, BRBReadyMessage.serializer);

        /*-------------------- Register Message Handlers ------------------------------- */
        registerMessageHandler(channelID, BRBSendMessage.MESSAGE_ID, this::uponRecieveSendMessage);
        registerMessageHandler(channelID, BRBEchoMessage.MESSAGE_ID, this::uponRecieveEchoMessage);
        registerMessageHandler(channelID, BRBReadyMessage.MESSAGE_ID, this::uponRecieveReadyMessage);

        /*-------------------- Register Request Handlers ------------------------------- */
        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);


        // Process neighbors and open connection to them (Copiado do staticmembership)
        String[] neighborsCandidates = props.getProperty(PAR_NEIGHBORS).split(",");
        for (int i = 0; i < neighborsCandidates.length; i++) {
            hostElements = neighborsCandidates[i].split(":");
            Host candidate = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));
            if (!candidate.equals(myself)) {
                this.candidates.add(candidate);
            }
        }

        //copiado do staticmembership
        for (Host h : this.candidates) {
            //This is a simple hack to ensure that only the node with highest port starts establishing the protocol
            if(h.getPort() < myself.getPort()) {
                openConnection(h);
            }
        }

        //copiado do static membership
        try {
            this.privateKey = Crypto.getPrivateKey(myIdentifier, props);
            this.truststore = Crypto.getTruststore(props);
            this.publicKey = this.truststore.getCertificate(myIdentifier).getPublicKey();
        } catch (Exception e) {
            System.err.println("Could not retrieve cryptographic material");
            e.printStackTrace();
            System.exit(1);
        }

    }

    private void handleAppendRequest(ProtoRequest protoRequest, short protoId) {
        AppendRequest req = (AppendRequest) protoRequest;

        //String mid = generateUniqueID(req.getPayload());

        //TODO
    }

    private void uponRecieveReadyMessage(ProtoMessage protoMessage, Host host, short protoId, int channel) {
        //TODO
    }

    private void uponRecieveEchoMessage(ProtoMessage protoMessage, Host host, short protoId, int channel) {
        //TODO
    }

    private void uponRecieveSendMessage(ProtoMessage protoMessage, Host host, short protoId, int channel) {
        //TODO
    }
}
