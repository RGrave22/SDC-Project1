package PBFT;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import PBFT.messages.CommitMessage;
import PBFT.messages.NewViewMessage;
import PBFT.messages.PrePrepareMessage;
import PBFT.messages.PrepareMessage;
import PBFT.messages.ViewChangeMessage;
import PBFT.notifications.CommitNotification;
import PBFT.notifications.NewViewNotification;
import PBFT.requests.AppendRequest;
import common.events.ChannelAvailable;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class PBFTAgreement extends GenericProtocol {

    public static final short PROTO_ID = 1007;
    public static final String PROTO_NAME = "PBFTAgreement";

    private final Map<Long, PBFTLogEntry> log;

    private static final long PRIMARY_TIMEOUT = 5000;
    private static final long PROGRESS_TIMEOUT = 10000;
    private long lastPrimaryResponseTime;
    private long lastProgressTime;

    private final Map<Long, Set<Host>> viewChangeVotes;
    private final Map<Long, List<ViewChangeMessage>> receivedViewChanges;
    private final Map<Long, List<PrepareMessage>> pendingPrepares = new ConcurrentHashMap<>();
    private final Map<Long, List<CommitMessage>> pendingCommits = new ConcurrentHashMap<>();
    private final Set<Long> startedViews;
    private final Set<Long> sentViewChange;
    private PrivateKey privateKey;
    private Map<Host, PublicKey> publicKeys;

    private int channelID;
    private List<Host> neighbors;
    private Host self;
    private Host primary;

    private long seqNo;
    private int f;
    private long currentView;

    public PBFTAgreement() {
        super(PROTO_NAME, PROTO_ID);
        this.seqNo = 0;
        this.f = 1;
        this.currentView = 0;
        this.log = new HashMap<>();
        this.lastPrimaryResponseTime = System.currentTimeMillis();
        this.lastProgressTime = System.currentTimeMillis();
        this.viewChangeVotes = new HashMap<>();
        this.receivedViewChanges = new HashMap<>();
        this.startedViews = new HashSet<>();
        this.sentViewChange = new HashSet<>();
        this.publicKeys = new HashMap<>();
        this.neighbors = new ArrayList<>();
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        registerRequestHandler(AppendRequest.REQUEST_ID, this::handleRequest);

        subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);

    }

    public void handleChannelAvailableNotification(ChannelAvailable notification, short sourceProto) {

        this.self = notification.getMyHost();
        this.privateKey = notification.getMyPrivateKey();
        this.channelID = notification.getChannelID();

        registerSharedChannel(channelID);
        setDefaultChannel(channelID);

        // Register Message Serializers
        registerMessageSerializer(channelID, PrePrepareMessage.MESSAGE_ID, PrePrepareMessage.serializer);
        registerMessageSerializer(channelID, PrepareMessage.MESSAGE_ID, PrepareMessage.serializer);
        registerMessageSerializer(channelID, CommitMessage.MESSAGE_ID, CommitMessage.serializer);
        registerMessageSerializer(channelID, ViewChangeMessage.MESSAGE_ID, ViewChangeMessage.serializer);
        registerMessageSerializer(channelID, NewViewMessage.MESSAGE_ID, NewViewMessage.serializer);

        // Setup Message Handlers
        try {
            registerMessageHandler(channelID, PrePrepareMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onPrePrepare((PrePrepareMessage) msg, from));
            registerMessageHandler(channelID, PrepareMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onPrepare((PrepareMessage) msg, from));
            registerMessageHandler(channelID, CommitMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onCommit((CommitMessage) msg, from));
            registerMessageHandler(channelID, ViewChangeMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onViewChange((ViewChangeMessage) msg, from));
            registerMessageHandler(channelID, NewViewMessage.MESSAGE_ID,
                    (msg, from, srcProto, channel) -> onNewView((NewViewMessage) msg, from));
        } catch (HandlerRegistrationException e) {
            throw new RuntimeException("Erro ao iniciar mensagens.");
        }

    }

    public void handleRequest(AppendRequest req, short sourceProto) {
        if (amPrimary()) {
            propose(req);
        }
    }

    public void updatePrimary() {
        if (neighbors.isEmpty()) {
            primary = null;
            return;
        }

        List<Host> sortedNeighbors = new ArrayList<>(neighbors);
        if (self != null) {
            sortedNeighbors.add(self);
        }
        Collections.sort(sortedNeighbors);

        Host newPrimary = sortedNeighbors.get((int) (currentView % sortedNeighbors.size()));
        this.primary = newPrimary;
        System.out.println("Primary atualizado: " + primary);
        onMessageFromPrimary();
    }

    private boolean amPrimary() {
        return self != null && primary != null && primary.equals(self);
    }

    public void propose(AppendRequest req) {
        System.err.println("Chega aqui: " + primary);
        System.err.println("É primary: " + self);
        if (!amPrimary()) {
            return;
        }

        boolean exists = log.values().stream()
                .anyMatch(e -> Arrays.equals(e.getRequest().getClientRequest(), req.getClientRequest()));
        if (exists) {
            System.out.println("Pedido já existe no log, ignorando novo propose.");
            return;
        }

        System.err.println("Chega aqui 2");

        long seq = ++seqNo;
        byte[] digest = SignaturesHelper.digest(req.getClientRequest());
        PBFTLogEntry entry = new PBFTLogEntry(seq, currentView, req, digest);
        log.put(seq, entry);

        PrePrepareMessage ppm = new PrePrepareMessage(self, currentView, seq, digest, req);
        broadcast(ppm);
    }

    public void setF(int newF) {
        this.f = newF;
    }

    public void setNeighbor(List<Host> newNeighbors) {
        this.neighbors = newNeighbors;
    }

    public void setPublicKeys(Map<Host, PublicKey> newPublickeys) {
        this.publicKeys = newPublickeys;
    }

    private void broadcast(ProtoMessage msg) {
        for (Host h : neighbors) {
            if (!h.equals(self)) {
                sendMessage(msg, h);
            }
        }
    }

    private void onPrePrepare(PrePrepareMessage msg, Host from) {

        if (!from.equals(primary)) {
            return;
        }

        System.out.println("Começa prePrepare");

        onMessageFromPrimary();

        byte[] digest = SignaturesHelper.digest(msg.getRequest().getClientRequest());
        if (!Arrays.equals(digest, msg.getDigest())) {
            System.out.println("Digest incorreto");
            startViewChange(currentView + 1);
            return;
        }

        if (log.containsKey(msg.getSeq())) {
            System.out.println("SeqNo já existente");
            startViewChange(currentView + 1);
            return;
        }

        PBFTLogEntry entry = new PBFTLogEntry(msg.getSeq(), msg.getView(), msg.getRequest(), digest);
        log.put(msg.getSeq(), entry);

        List<PrepareMessage> pendPrepares = pendingPrepares.remove(msg.getSeq());
        if (pendPrepares != null) {
            for (PrepareMessage pm : pendPrepares) {
                onPrepare(pm, pm.getSender());
            }
        }

        PrepareMessage prepare = new PrepareMessage(msg.getView(), msg.getSeq(), digest, self);
        broadcast(prepare);

        List<CommitMessage> pendCommits = pendingCommits.remove(msg.getSeq());
        if (pendCommits != null) {
            for (CommitMessage cm : pendCommits) {
                onCommit(cm, cm.getSender());
            }
        }

    }

    private void onPrepare(PrepareMessage msg, Host from) {

        System.out.println("KeySet: " + log.keySet());
        System.out.println("Seq: " + msg.getSeq());

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            pendingPrepares.computeIfAbsent(msg.getSeq(), k -> new ArrayList<>()).add(msg);
            return;
        }

        System.out.println("Req: " + entry.getRequest().getRequestID());
        System.out.println("Começa Prepare");

        entry.addPrepare(from);
        if (entry.getPreparesCount() > 2 * f) {
            CommitMessage cm = new CommitMessage(msg.getView(), msg.getSeq(), entry.getDigest(), self);
            broadcast(cm);
        }
    }

    private void onCommit(CommitMessage msg, Host from) {

        System.out.println("Começa commit: " + msg.getSeq());

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            pendingCommits.computeIfAbsent(msg.getSeq(), k -> new ArrayList<>()).add(msg);
            return;
        }

        System.out.println("Req: " + entry.getRequest().getRequestID());
        System.out.println("Commit minimos: " + ((2 * f) + 1));
        System.out.println("Commit Count: " + entry.getCommitsCount());
        System.out.println("IsDelivered: " + entry.isDelivered());

        entry.addCommit(from);

        System.out.println("Commit Count (depois): " + entry.getCommitsCount());

        if (entry.getCommitsCount() >= ((2 * f) + 1) && !entry.isDelivered()) {
            entry.markDelivered();
            AppendRequest req = entry.getRequest();
            lastProgressTime = System.currentTimeMillis();
            triggerNotification(new CommitNotification(entry.getSeq(), req));

        } else if (!entry.hasCommitFrom(self) && !entry.isDelivered()) {
            CommitMessage myCommit = new CommitMessage(
                    msg.getView(),
                    msg.getSeq(),
                    msg.getDigest(),
                    self);
            broadcast(myCommit);
        }
    }

    public void onViewChange(ViewChangeMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            byte[] data = ViewChangeMessage.getBytesForSigning(msg);
            if (pub == null || !SignaturesHelper.checkSignature(data, msg.getSign(), pub)) {
                return;
            }
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            return;
        }

        long v = msg.getNovaView();

        receivedViewChanges.computeIfAbsent(v, k -> new ArrayList<>()).add(msg);
        viewChangeVotes.computeIfAbsent(v, k -> new HashSet<>()).add(from);

        Set<Host> votes = viewChangeVotes.get(v);

        System.err.println("View Change votes for view " + v + ": " + votes.size());

        // devia ser 2f
        if (votes.size() >= (2 * f + 1)) {

            if (v > currentView) {
                currentView = v;
            }

            updatePrimary();
            System.err.println("New primary after view change: " + primary);
            if (amPrimary() && !startedViews.contains(v)) {
                startedViews.add(v);
                startNewView(v);
            }
        }

        else if (!amPrimary() && votes.size() >= f + 1 && !sentViewChange.contains(v)) {
            startViewChange(v);
            sentViewChange.add(v);
        }
    }

    private void startViewChange(long v) {

        List<PrePrepareMessage> prePrePare = collectPendingPrePrepares(v);
        List<CommitMessage> commits = collectCommits(v);

        byte[] sign = null;

        ViewChangeMessage vcm = new ViewChangeMessage(v, self, sign, commits, prePrePare);

        try {
            byte[] data = ViewChangeMessage.getBytesForSigning(vcm);
            sign = SignaturesHelper.generateSignature(data, privateKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException("Erro ao assinar a mensagem", e);
        }

        vcm.setSign(sign);

        System.out.println("A iniciar view change para a view: " + self);

        broadcast(vcm);
    }

    private void startNewView(long v) {

        List<ViewChangeMessage> vChanges = receivedViewChanges.get(v);
        List<PrePrepareMessage> prePrepares = collectPendingPrePrepares(v);

        byte[] sign = null;

        NewViewMessage nvm = new NewViewMessage(v, vChanges, prePrepares, sign);

        try {
            byte[] data = NewViewMessage.getBytesForSigning(nvm);
            sign = SignaturesHelper.generateSignature(data, privateKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            throw new RuntimeException("Erro ao assinar a mensagem new view message", e);
        }

        nvm.setSign(sign);

        broadcast(nvm);

        System.err.println("Primary mandou mensagem de nova view: " + v);

        triggerNotification(new NewViewNotification(v));

        for (PBFTLogEntry entry : log.values()) {
            if (!entry.isDelivered()) {
                propose(entry.getRequest());
            }
        }

    }

    public void onNewView(NewViewMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            byte[] data = NewViewMessage.getBytesForSigning(msg);
            if (pub == null || !SignaturesHelper.checkSignature(data, msg.getSign(), pub)) {
                return;
            }
        } catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            return;
        }

        onMessageFromPrimary();

        for (PrePrepareMessage ppm : msg.getPrePreparedChanges()) {
            if (!log.containsKey(ppm.getSeq())) {
                PBFTLogEntry entry = new PBFTLogEntry(ppm.getSeq(), ppm.getView(), ppm.getRequest(), ppm.getDigest());
                log.put(ppm.getSeq(), entry);
                PrepareMessage prepare = new PrepareMessage(ppm.getView(), ppm.getSeq(), ppm.getDigest(), self);
                broadcast(prepare);
            }
        }

        currentView = msg.getNovaView();
        updatePrimary();

    }

    public void onMessageFromPrimary() {
        System.out.println("Primary respondeu: ");
        lastPrimaryResponseTime = System.currentTimeMillis();
    }

    public void checkPrimaryTimeout() {
        boolean noPrimaryResponse = (System.currentTimeMillis() - lastPrimaryResponseTime > PRIMARY_TIMEOUT);
        boolean noProgress = (System.currentTimeMillis() - lastProgressTime > PROGRESS_TIMEOUT);
        System.out.println("No Primary Response: " + noPrimaryResponse);
        if (!amPrimary() && !neighbors.isEmpty() && (noPrimaryResponse || noProgress)) {
            startViewChange(currentView + 1);
        }
    }

    private List<PrePrepareMessage> collectPendingPrePrepares(long newView) {
        List<PrePrepareMessage> pending = new ArrayList<>();
        for (PBFTLogEntry e : log.values()) {
            if (!e.isDelivered() && e.getView() <= newView) {
                PrePrepareMessage ppm = new PrePrepareMessage(self, e.getView(), e.getSeq(), e.getDigest(),
                        e.getRequest());
                pending.add(ppm);
            }
        }
        return pending;
    }

    private List<CommitMessage> collectCommits(long newView) {
        List<CommitMessage> done = new ArrayList<>();
        for (PBFTLogEntry e : log.values()) {
            if (e.isDelivered() && e.getView() <= newView) {
                CommitMessage cm = new CommitMessage(e.getView(), e.getSeq(), e.getDigest(),
                        self);
                done.add(cm);
            }
        }
        return done;
    }

}
