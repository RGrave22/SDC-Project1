package byzantinereliablebroadcast;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;
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

        // proceder com a implementação (acho que o proximo passo é ja os handlers de notificaçoes e mensagens)





    }
}
