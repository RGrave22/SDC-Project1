package byzantinereliablebroadcast;

import app.messages.client.AppendToLog;
import byzantinereliablebroadcast.messages.BRBEchoMessage;
import byzantinereliablebroadcast.messages.BRBReadyMessage;
import byzantinereliablebroadcast.messages.BRBSendMessage;
import byzantinereliablebroadcast.messages.ConnectionMessage;
import byzantinereliablebroadcast.notifications.DeliverEntryNotification;
import common.events.ChannelAvailable;
import io.netty.util.internal.SocketUtils;
import membership.staticmembership.message.HelloMessage;
import membership.staticmembership.message.HelloReplyMessage;
import membership.staticmembership.message.ReplyMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.babel.generic.signed.InvalidSerializerException;
import pt.unl.fct.di.novasys.babel.handlers.MessageInHandler;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
import simplereplication.requests.AppendRequest;
import utils.Crypto;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.Channel;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private HashMap<String, Boolean> sentReady;
    private HashMap<String, HashSet<Host>> readys;

    private int pi;

    private static final Logger logger = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);

    public ByzantineReliableBroadcastProtocol() {
        super(PROTO_NAME, PROTO_ID);

//        this.neighbors = new HashMap<Host, PublicKey>();
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
        sentReady = new HashMap<>();
        readys = new HashMap<>();

        this.candidates = new  HashSet<>();

        this.pi = 0;

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

        //TODO perguntar se como se trata de BRB e nao Byzantine Consistent Broadcast se nao é preciso o ready
        //Nao sei se esta correto
        /*-------------------- Register Message Serializers ------------------------------- */
        registerMessageSerializer(channelID, BRBSendMessage.MESSAGE_ID, BRBSendMessage.serializer);
        registerMessageSerializer(channelID, BRBEchoMessage.MESSAGE_ID, BRBEchoMessage.serializer);
        registerMessageSerializer(channelID, BRBReadyMessage.MESSAGE_ID, BRBReadyMessage.serializer);
        //registerMessageSerializer(channelID, ConnectionMessage.MESSAGE_ID, ConnectionMessage.serializer);

        /*-------------------- Register Message Handlers ------------------------------- */
        registerMessageHandler(channelID, BRBSendMessage.MESSAGE_ID, this::uponReceiveSendMessage);
        registerMessageHandler(channelID, BRBEchoMessage.MESSAGE_ID, this::uponReceiveEchoMessage);
        registerMessageHandler(channelID, BRBReadyMessage.MESSAGE_ID, this::uponRecieveReadyMessage);
//        registerMessageHandler(channelID, ConnectionMessage.MESSAGE_ID, this::uponReceiveChannelAvailable);

        /*-------------------- Register Request Handlers ------------------------------- */
        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);

        //subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::uponReceiveChannelAvailable);


        // Process neighbors and open connection to them (Copiado do staticmembership)
        String[] neighborsCandidates = props.getProperty(PAR_NEIGHBORS).split(",");
        for (int i = 0; i < neighborsCandidates.length; i++) {
            hostElements = neighborsCandidates[i].split(":");
            Host candidate = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));
            if (!candidate.equals(myself)) {
                this.candidates.add(candidate);
            }
        }

        System.out.println("Neighbours: " + candidates.toString() + " size: " + candidates.size());

        //numero de processos existentes
        pi = candidates.size() + 1;



        //copiado do staticmembership
        int i = 1;
        for (Host h : this.candidates) {
            //This is a simple hack to ensure that only the node with highest port starts establishing the protocol
            if(h.getPort() < myself.getPort()) {
                System.out.println(i);
                openConnection(h);
                i++;
                System.out.println(h);
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

        myIdentifier = props.getProperty(Crypto.CRYPTO_NAME_KEY);
        logger.info("Initializing BRB protocol for node {}", myIdentifier);

        triggerNotification(new ChannelAvailable(TCPChannel.NAME, channelID, myself, privateKey, publicKey));

    }




    private void handleAppendRequest(ProtoRequest protoRequest, short protoId) {
        //TODO Nao sei se é necessario todo este aparato para criar o mid, se nao, é mais facil
        //TODO Fará sendito a BRB message, em vez de enviar um byte[] mandar mesmo objeto AppendTolog?
        try{
            AppendRequest req = (AppendRequest) protoRequest;

            byte[] bs = req.getClientRequest();
            AppendToLog atL = AppendToLog.deserializeRequest(bs);

            String msg = atL.getLogEntry();
            System.out.println(msg);

//            String mid = UUID.fromString(msg).toString(); //ou a função auxiliar que fiz
            String mid = UUID.randomUUID().toString();

            logger.info("Received AppendRequest from client {}: logEntry='{}', generated MID={}",
                    atL.getClientID(), msg, mid);

            BRBSendMessage brbsendmsg = new BRBSendMessage(myself, mid, bs);

            //brbsendmsg.signMessage(privateKey);

            for (Host h : candidates) {
                System.out.println(h);
                sendMessage(brbsendmsg, h);
            }

           //"envia" para o proprio
            uponReceiveSendMessage(brbsendmsg, myself, PROTO_ID, channelID);

        } catch (IOException e) {
            throw new RuntimeException(e);
//        } catch (NoSuchAlgorithmException e) {
//            throw new RuntimeException(e);
//        } catch (InvalidSerializerException e) {
//            throw new RuntimeException(e);
//        } catch (SignatureException e) {
//            throw new RuntimeException(e);
//        } catch (InvalidKeyException e) {
//            throw new RuntimeException(e);
        }

    }

    private void uponRecieveReadyMessage(ProtoMessage protoMessage, Host sender, short protoId, int channel) {
        BRBReadyMessage msg = (BRBReadyMessage) protoMessage;
        logger.info("Received READY: " + msg.getPayload() + " ID: " + msg.getMessageId() + " HOST: " + sender.getAddress() + ":" + sender.getPort());


        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();

        readys.putIfAbsent(mid, new HashSet<>());
        HashSet<Host> readyHosts = readys.get(mid);



        if(!readyHosts.contains(sender)) {
            readyHosts.add(sender);

            System.out.println("TAMANHO DO READY HOSTS: " + readyHosts.size());


            int f = (pi - 1) / 3;
            sentReady.putIfAbsent(mid, false);
            if (!sentReady.get(mid) && (readyHosts.size() > f)) {
                sentReady.put(mid, true);

                BRBReadyMessage readyMsg = new BRBReadyMessage(myself, mid, payload);

                for (Host h : candidates) {
//                try {
//                    readyMsg.sign(privateKey);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    continue;
//                }
                    sendMessage(readyMsg, h);
                }
            }


            if (!delivered.contains(mid) && (readyHosts.size() > 2 * f)) {
                delivered.add(mid);

                try {
                    AppendToLog atL = AppendToLog.deserializeRequest(payload);
                    DeliverEntryNotification notification = new DeliverEntryNotification(UUID.randomUUID(), atL.getClientID(), payload, null);
                    //notification.sign(privateKey);

                    logger.info("Delivered entry MID={} from client {} via BRB: logEntry='{}'",
                            mid, atL.getClientID(), atL.getLogEntry());

                    triggerNotification(notification);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void uponReceiveEchoMessage(ProtoMessage protoMessage, Host sender, short protoId, int channel) {
        BRBEchoMessage msg = (BRBEchoMessage) protoMessage;
        logger.info("Received ECHO: " + msg.getPayload().toString() + "ID: " + msg.getMessageId() + "HOST: " + sender.getAddress() + ":" + sender.getPort());

        //Não sei bem se é necessario
//        try {
//
//            if (!msg.verifySignature(publicKeys.get(sender))) {
//                System.err.println("Assinatura inválida do echo de " + sender);
//                return;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            return;
//        }

        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();


        echoMsgs.putIfAbsent(mid, new HashSet<>());
        HashSet<Host> echoes = echoMsgs.get(mid);


        if (!echoes.contains(sender)) {
            echoes.add(sender);


            int f = (pi - 1) / 3;
            sentReady.putIfAbsent(mid, false);
            //IMPORTANTE - mudei a formula, porque a formula que estava levava a metades (2,5) assim nao ha metades
            if ((echoes.size() >= ((2*f) +1)) && !sentReady.get(mid)) {
    //            delivered.add(mid);

                try {
    //                AppendToLog atL = AppendToLog.deserializeRequest(payload);
                    //Não sei se ta bem criado
    //                DeliverEntryNotification notification = new DeliverEntryNotification(UUID.fromString(mid), atL.getClientID(), payload, null);
    //                notification.sign(privateKey);

                    sentReady.put(mid, true);

                    BRBReadyMessage readyMsg = new BRBReadyMessage(myself, mid, payload);

                    for (Host h : candidates) {
                        if (!h.equals(myself)) {
                            //logger.info("Sent BRBReadyMessage MID={} to {}", mid, h);
                            System.out.println("Sent READY TO: " + h.getAddress() + ":" + h.getPort() + " ID: " + mid);
                            sendMessage(readyMsg, h);
                        }
                    }

                    uponRecieveReadyMessage(readyMsg, myself, protoId, channel);


                    //triggerNotification(notification);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //TODO AINDA NAO SEI SE ESTA BEM
    private void uponReceiveSendMessage(ProtoMessage protoMessage, Host host, short protoId, int channel) {
        BRBSendMessage msg = (BRBSendMessage) protoMessage;
        System.out.println("Received SEND: " + msg.getPayload().toString() + " ID: " + msg.getMessageId() + " HOST: " + host.getAddress() + ":" + host.getPort());

        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();


        echoMsgs.putIfAbsent(mid, new HashSet<>());

        HashSet<Host> echoes = echoMsgs.get(mid);

        if(!echoes.contains(myself)) {
            echoes.add(myself);


            BRBEchoMessage echoMsg = new BRBEchoMessage(myself, mid, payload);

//            try {
//                echoMsg.signMessage(privateKey);
//            } catch (Exception e) {
//                //logger.error("Error signing ECHO message for mid " + mid, e);
//                return;
//            }


            for (Host h : candidates) {
                if (!h.equals(myself)) {
                    //logger.info("Sent BRBEchoMessage MID={} to {}", mid, h);
                    System.out.println("Sent ECHO TO: " + h.getAddress() + ":" + h.getPort() + " ID: " + mid);
                    sendMessage(echoMsg, h);
                }
            }


            //uponReceiveEchoMessage(echoMsg, myself, protoId, channel);

        }


    }

    //auxiliar para gerar id, acho que nao vai ser preciso
    private String generateUniqueID(String payload) throws NoSuchAlgorithmException {
        //Podemos fazer assim? ou podemos ter apenas um counter para os ids?
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(payload.getBytes(StandardCharsets.UTF_8));


        StringBuilder hexString = new StringBuilder();
        for (byte b : digest) {
            hexString.append(String.format("%02x", b));
        }

        System.out.println("MD5 hash: " + hexString.toString());



        return hexString.toString();
    }

}
