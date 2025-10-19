package blockchainBRB;

import app.messages.client.AppendToLog;
import blockchainBRB.messages.BRBEchoMessage;
import blockchainBRB.messages.BRBReadyMessage;
import blockchainBRB.messages.BRBSendMessage;
import blockchainBRB.messages.HelloMessage;
import blockchainBRB.messages.HelloReplyMessage;
import blockchainBRB.messages.ReplyMessage;
import blockchainBRB.notifications.DeliverEntryNotification;
import common.events.ChannelAvailable;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.babel.generic.signed.InvalidSerializerException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import blockchainBRB.requests.AppendRequest;
import utils.Crypto;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.SignaturesHelper;

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

    private final Logger logger = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);

    private PrivateKey privateKey;
    private KeyStore truststore;
    private PublicKey publicKey;
    private String myIdentifier;

    private HashMap<Host, Long> issuedChallenges;

    //BRB
    private HashMap<String, HashSet<Host>> echoMsgs;
    private HashSet<String> delivered;
    private HashMap<String, Boolean> sentReady;
    private HashMap<String, HashSet<Host>> readys;

    public ByzantineReliableBroadcastProtocol() {
        super(PROTO_NAME, PROTO_ID);

        this.neighbors = new HashMap<Host, PublicKey>();
        this.candidates = new HashSet<Host>();
        this.publicKeys = new HashMap<Host, PublicKey>();

        this.issuedChallenges = new HashMap<Host, Long>();

        this.myself = null;

        this.privateKey = null;
        this.truststore = null;
        this.publicKey = null;

        this.myIdentifier = null;

        //BRB
        echoMsgs = new HashMap<>();
        delivered = new HashSet<>();
        sentReady = new HashMap<>();
        readys = new HashMap<>();

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

        Properties channelProps = new Properties();
        channelProps.put(TCPChannel.ADDRESS_KEY, this.myself.getAddress().toString().replace("/", ""));
        channelProps.put(TCPChannel.PORT_KEY, this.myself.getPort() + "");

        channelID = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelID, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelID, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelID, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelID, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelID, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*-------------------- Register Message Serializers ------------------------------- */
        registerMessageSerializer(channelID, HelloMessage.MESSAGE_ID, HelloMessage.serializer);
        registerMessageSerializer(channelID, HelloReplyMessage.MESSAGE_ID, HelloReplyMessage.serializer);
        registerMessageSerializer(channelID, ReplyMessage.MESSAGE_ID, ReplyMessage.serializer);

        registerMessageSerializer(channelID, BRBSendMessage.MESSAGE_ID, BRBSendMessage.serializer);
        registerMessageSerializer(channelID, BRBEchoMessage.MESSAGE_ID, BRBEchoMessage.serializer);
        registerMessageSerializer(channelID, BRBReadyMessage.MESSAGE_ID, BRBReadyMessage.serializer);

        /*-------------------- Register Message Handlers ------------------------------- */
        registerMessageHandler(channelID, HelloMessage.MESSAGE_ID, this::uponReceiveHelloMessage);
        registerMessageHandler(channelID, HelloReplyMessage.MESSAGE_ID, this::uponReceiveHelloReplyMessage);
        registerMessageHandler(channelID, ReplyMessage.MESSAGE_ID, this::uponReceiveReplyMessage);

        registerMessageHandler(channelID, BRBSendMessage.MESSAGE_ID, this::uponReceiveSendMessage);
        registerMessageHandler(channelID, BRBEchoMessage.MESSAGE_ID, this::uponReceiveEchoMessage);
        registerMessageHandler(channelID, BRBReadyMessage.MESSAGE_ID, this::uponReceiveReadyMessage);

        /*-------------------- Register Request Handlers ------------------------------- */
        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);

        // Process neighbors and open connection to them

        String[] neighborsCandidates = props.getProperty(PAR_NEIGHBORS).split(",");
        for (int i = 0; i < neighborsCandidates.length; i++) {
            hostElements = neighborsCandidates[i].split(":");
            Host candidate = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));
            if (!candidate.equals(myself)) {
                this.candidates.add(candidate);
            }
        }

        for (Host h : this.candidates) {
            //This is a simple hack to ensure that only the node with highest port starts establishing the protocol
            if(h.getPort() < myself.getPort()) {
                openConnection(h);
            }
        }

        try {
            this.privateKey = Crypto.getPrivateKey(myIdentifier, props);
            this.truststore = Crypto.getTruststore(props);
            this.publicKey = this.truststore.getCertificate(myIdentifier).getPublicKey();
        } catch (Exception e) {
            System.err.println("Could not retrieve cryptographic material");
            e.printStackTrace();
            System.exit(1);
        }

        triggerNotification(new ChannelAvailable(TCPChannel.NAME, channelID, myself, privateKey, publicKey));
    }

    private void handleAppendRequest(ProtoRequest protoRequest, short protoId) {
        try{
            AppendRequest req = (AppendRequest) protoRequest;

            byte[] bs = req.getClientRequest();
            AppendToLog atL = AppendToLog.deserializeRequest(bs);

            String msg = atL.getLogEntry();
            long timestamp = atL.getTimestamp();
            System.out.println(msg);


            //Qual a melhor opçao? TODO Perguntar ao stor
//            String mid = UUID.randomUUID().toString();
            String mid = generateUniqueID(msg,timestamp);

            logger.info("Received AppendRequest from client {}: logEntry='{}', generated MID={}",
                    atL.getClientID(), msg, mid);

            BRBSendMessage brbsendmsg = new BRBSendMessage(myself, mid, bs);

            brbsendmsg.signMessage(privateKey);

            for (Host h : neighbors.keySet()) {
                System.out.println(h);
                sendMessage(brbsendmsg, h);
            }

            //simula o envio para o proprio
            uponReceiveSendMessage(brbsendmsg, myself, PROTO_ID, channelID);

        } catch (IOException e) {
            throw new RuntimeException(e);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidSerializerException e) {
            throw new RuntimeException(e);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }

    }

    private void uponReceiveReadyMessage(ProtoMessage protoMessage, Host sender, short protoId, int channel) {
        BRBReadyMessage msg = (BRBReadyMessage) protoMessage;
        logger.info("Received READY: " + msg.getPayload() + " ID: " + msg.getMessageId() + " HOST: " + sender.getAddress() + ":" + sender.getPort());

        //check the incoming message signature
        if(this.publicKeys.containsKey(msg.getSender()) && !sender.equals(myself)) {
            try {
                if(!msg.checkSignature(this.publicKeys.get(msg.getSender()))){
                    throw new Exception();
                }
            } catch (Exception e) {
                logger.error("Could not verify authenticity of BroadcastMessage sent by " + msg.getSender());
                e.printStackTrace();
            }
        } else if (msg.getSender().equals(myself)) {
            logger.info("Message received from myself, no need to check the signature.");
        } else {
            logger.warn("I do not know the public key of " + msg.getSender() + " to validate the message.");
        }

        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();

        readys.putIfAbsent(mid, new HashSet<>());
        HashSet<Host> readyHosts = readys.get(mid);

        if(!readyHosts.contains(sender)) {
            readyHosts.add(sender);

            System.out.println("TAMANHO DO READY HOSTS: " + readyHosts.size());


            int pi = neighbors.keySet().size() + 1;
            int f = (pi - 1) / 3;
            sentReady.putIfAbsent(mid, false);
            if (!sentReady.get(mid) && (readyHosts.size() > f)) {
                sentReady.put(mid, true);

                BRBReadyMessage readyMsg = new BRBReadyMessage(myself, mid, payload);

                //sign the ready message
                try {
                    readyMsg.signMessage(privateKey);
                    for (Host h : neighbors.keySet()) {
                        sendMessage(readyMsg, h);
                    }
                } catch (InvalidSerializerException | NoSuchAlgorithmException | SignatureException |
                         InvalidKeyException e) {
                    throw new RuntimeException(e);
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

        System.out.println(publicKeys.keySet());

        if(this.publicKeys.containsKey(msg.getSender()) && !msg.getSender().equals(myself)) {
            try {
                if (!msg.checkSignature(publicKeys.get(sender))) {
                    System.err.println("Assinatura inválida do echo de " + sender);
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        } else if (msg.getSender().equals(myself)) {
            logger.info("Message received from myself, no need to check the signature.");
        } else {
            logger.warn("I do not know the public key of " + msg.getSender() + " to validate the message.");
        }


        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();


        echoMsgs.putIfAbsent(mid, new HashSet<>());
        HashSet<Host> echoes = echoMsgs.get(mid);


        if (!echoes.contains(sender)) {
            echoes.add(sender);

            int pi = neighbors.keySet().size() + 1;
            int f = (pi - 1) / 3;
            sentReady.putIfAbsent(mid, false);
            //IMPORTANTE - mudei a formula, porque a formula que estava levava a metades (2,5) assim nao ha metades
            if ((echoes.size() >= ((2*f) + 1)) && !sentReady.get(mid)) {
                //            delivered.add(mid);

                try {

                    sentReady.put(mid, true);

                    BRBReadyMessage readyMsg = new BRBReadyMessage(myself, mid, payload);

                    readyMsg.signMessage(privateKey);



                    for (Host h : neighbors.keySet()) {
                        if (!h.equals(myself)) {
                            //logger.info("Sent BRBReadyMessage MID={} to {}", mid, h);
                            System.out.println("Sent READY TO: " + h.getAddress() + ":" + h.getPort() + " ID: " + mid);
                            sendMessage(readyMsg, h);
                        }
                    }

                    uponReceiveReadyMessage(readyMsg, myself, protoId, channel);

                } catch (InvalidSerializerException | NoSuchAlgorithmException | SignatureException |
                         InvalidKeyException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //TODO meter && host != myself na verificação da chave
    private void uponReceiveSendMessage(ProtoMessage protoMessage, Host sender, short protoId, int channel) {
        BRBSendMessage msg = (BRBSendMessage) protoMessage;
        System.out.println("Received SEND: " + msg.getPayload().toString() + " ID: " + msg.getMessageId() + " HOST: " + sender.getAddress() + ":" + sender.getPort());

        String mid = msg.getMessageId();
        byte[] payload = msg.getPayload();

        System.out.println("KEYSET: " + publicKeys.keySet());

        System.out.println("Sender da msg: " + msg.getSender());
        System.out.println("mySelf: " + myself);
        System.out.println("sender: " + sender);

        if(this.publicKeys.containsKey(msg.getSender())) {
            try {
                if (!msg.checkSignature(this.publicKeys.get(msg.getSender()))) {
                    throw new Exception();
                }
            } catch (Exception e) {
                logger.error("Could not verify authenticity of BroadcastMessage sent by " + msg.getSender());
                e.printStackTrace();
            }
        } else if (msg.getSender().equals(myself)) {
                logger.info("Message received from myself, no need to check the signature.");
        } else {
                logger.warn("I do not know the public key of " + msg.getSender() + " to validate the message.");
            }


        echoMsgs.putIfAbsent(mid, new HashSet<>());

        HashSet<Host> echoes = echoMsgs.get(mid);

        if(!echoes.contains(myself)) {
            echoes.add(myself);


            BRBEchoMessage echoMsg = new BRBEchoMessage(myself, mid, payload);

            try {
                echoMsg.signMessage(privateKey);



                for (Host h : neighbors.keySet()) {
                    if (!h.equals(myself)) {
                        //logger.info("Sent BRBEchoMessage MID={} to {}", mid, h);
                        System.out.println("Sent ECHO TO: " + h.getAddress() + ":" + h.getPort() + " ID: " + mid);
                        sendMessage(echoMsg, h);
                    }
                }

            } catch (Exception e) {
                //logger.error("Error signing ECHO message for mid " + mid, e);
                return;
            }

            uponReceiveEchoMessage(echoMsg, myself, protoId, channel);
        }


    }


    private String generateUniqueID(String payload, long timestamp) throws NoSuchAlgorithmException {
        String msg = payload + "-" + timestamp;

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(msg.getBytes(StandardCharsets.UTF_8));


        StringBuilder hexString = new StringBuilder();
        for (byte b : digest) {
            hexString.append(String.format("%02x", b));
        }

        System.out.println("MD5 hash: " + hexString.toString());

        return hexString.toString();
    }

    /*
     * --------------------------------- Message Handlers
     * ----------------------------
     */

    private void uponReceiveHelloMessage(HelloMessage msg, Host sender, short protoID, int channel) {
        if (!sender.equals(msg.getSender())) {
            logger.error("Received a HelloMessage from the socker of " + sender + " issued by " + msg.getSender());
            return;
        }

        this.publicKeys.put(msg.getSender(), msg.getPubKey());
        long hisChallenge = msg.getChallenge();
        long myChallenge = new Random(System.currentTimeMillis()).nextLong();
        this.issuedChallenges.put(msg.getSender(), myChallenge);

        byte[] challengeAnswer = null;
        try {
            logger.info("Computing answer for challenge put foward by " + msg.getSender() + " which is: " + msg.getChallenge());
            Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
            sig.initSign(privateKey);
            sig.update(ByteBuffer.allocate(Long.BYTES).putLong(hisChallenge).array());
            challengeAnswer = sig.sign();
        } catch (Exception e) {
            logger.error("Could not generate a valid answer to the challenge of " + msg.getSender());
            e.printStackTrace();
        }

        logger.info("Answering challenge put fowards by " + msg.getSender() + " sending my Challenge: " + myChallenge);
        HelloReplyMessage hrm = new HelloReplyMessage(myself, publicKey, challengeAnswer, myChallenge);

        try {
            hrm.signMessage(privateKey);
            sendMessage(hrm , msg.getSender());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void uponReceiveHelloReplyMessage(HelloReplyMessage msg, Host sender, short protoID, int channel) {
        if (!sender.equals(msg.getSender())) {
            logger.error("Received a HelloReplyMessage from the socker of " + sender + " issued by " + msg.getSender());
            return;
        }

        // Validate his answer to my challenge
        try {
            logger.info("Verifying answer for my challenge to " + msg.getSender() + " which is: " + issuedChallenges.get(msg.getSender()));
            Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
            sig.initVerify(msg.getPubKey());
            sig.update(ByteBuffer.allocate(Long.BYTES).putLong(issuedChallenges.get(msg.getSender())).array());
            if (sig.verify(msg.getChallengeReply())) {
                logger.info("Challenge has been correctly answered by " + msg.getSender());
                issuedChallenges.remove(msg.getSender());
                this.candidates.remove(msg.getSender());
                this.neighbors.put(msg.getSender(), msg.getPubKey());
                this.publicKeys.put(msg.getSender(), msg.getPubKey());
                triggerNotification(new NeighborUp(msg.getSender(), this.neighbors.get(msg.getSender())));
            } else {
                logger.error("Challenge was incorrectly answered by " + msg.getSender());
                closeConnection(msg.getSender());
                return;
            }
        } catch (Exception e) {
            logger.info("Error while validating the challenge answer of " + msg.getSender());
            e.printStackTrace();
            closeConnection(msg.getSender());
            return;
        }

        // Generate an answer to his challenge
        byte[] challengeAnswer = null;
        try {
            logger.info("Computing answer for challenge put foward by " + msg.getSender() + " which is: " + msg.getChallenge());
            Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
            sig.initSign(privateKey);
            sig.update(ByteBuffer.allocate(Long.BYTES).putLong(msg.getChallenge()).array());
            challengeAnswer = sig.sign();
        } catch (Exception e) {
            logger.error("Could not generate a valid answer to the challenge of " + msg.getSender());
            e.printStackTrace();
        }

        // send the reply
        ReplyMessage rm = new ReplyMessage(myself, challengeAnswer);
        try {
            rm.signMessage(privateKey);
            sendMessage(rm, msg.getSender());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void uponReceiveReplyMessage(ReplyMessage msg, Host sender, short protoID, int channel) {
        if (!sender.equals(msg.getSender())) {
            logger.error("Received a ReplyMessage from the socker of " + sender + " issued by " + msg.getSender());
            return;
        }

        // Validate his answer to my challenge
        try {
            logger.info("Verifying answer for my challenge to " + msg.getSender() + " which is: " + issuedChallenges.get(msg.getSender()));
            Signature sig = Signature.getInstance(SignaturesHelper.SignatureAlgorithm);
            sig.initVerify(publicKeys.get(msg.getSender()));
            sig.update(ByteBuffer.allocate(Long.BYTES).putLong(issuedChallenges.get(msg.getSender())).array());
            if (sig.verify(msg.getChallengeReply())) {
                logger.info("Challenge has been correctly answered by " + msg.getSender());
                issuedChallenges.remove(msg.getSender());
                this.candidates.remove(msg.getSender());
                this.neighbors.put(msg.getSender(), publicKeys.get(msg.getSender()));
                //triggerNotification(new NeighborUp(msg.getSender(), this.neighbors.get(msg.getSender())));
            } else {
                logger.error("Challenge was incorrectly answered by " + msg.getSender());
                closeConnection(msg.getSender());
                return;
            }
        } catch (Exception e) {
            logger.info("Error while validating the challenge answer of " + msg.getSender());
            e.printStackTrace();
            closeConnection(msg.getSender());
            return;
        }
    }

    /*
     * --------------------------------- Channel Events ----------------------------
     */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.info("Host {} is down, cause: {}", event.getNode(), event.getCause());
        if (this.candidates.contains(event.getNode()))
            this.candidates.remove(event.getNode());
        else if (this.neighbors.containsKey(event.getNode())) {
            this.neighbors.remove(event.getNode());
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        logger.info("Connection to host {} failed, cause: {}", event.getNode(), event.getCause());
        this.candidates.remove(event.getNode());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Host (out) {} is up", event.getNode());
        if (this.candidates.remove(event.getNode())) {
            this.neighbors.put(event.getNode(), null);
            this.issuedChallenges.put(event.getNode(), new Random(System.currentTimeMillis()).nextLong());

            logger.info("Sending a challenge to " + event.getNode() + ": " + this.issuedChallenges.get(event.getNode()));

            HelloMessage hm = new HelloMessage(myself, publicKey, this.issuedChallenges.get(event.getNode()));
            try {
                hm.signMessage(privateKey);
                sendMessage(hm,event.getNode());
                logger.info("Out connection is up for " + event.getNode() + " sent challenge...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            logger.info("Out connection is up for " + event.getNode() + " but node is not part of candidates.");
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Host (in) {} is up", event.getNode());
        if (!this.neighbors.containsKey(event.getNode())) {
            this.candidates.remove(event.getNode());
            this.neighbors.put(event.getNode(), null);
            openConnection(event.getNode());
        }
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
