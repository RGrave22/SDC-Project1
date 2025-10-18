package PBFT;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import PBFT.messages.CommitMessage;
import PBFT.messages.NewViewMessage;
import PBFT.messages.PrePrepareMessage;
import PBFT.messages.PrepareMessage;
import PBFT.messages.ViewChangeMessage;
import PBFT.requests.AppendRequest;
import SMR.SMReplicationProtocol;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class PBFTAgreement {

    private final GenericProtocol parent;
    private final Map<Long, PBFTLogEntry> log = new HashMap<>();

    private static final long PRIMARY_TIMEOUT = 5000;
    private long lastPrimaryResponseTime = System.currentTimeMillis();

    private final Map<Long, Set<Host>> viewChangeVotes = new HashMap<>();
    private final Map<Long, List<ViewChangeMessage>> receivedViewChanges = new HashMap<>();
    private final Set<Long> startedViews = new HashSet<>();
    private Set<Long> sentViewChange = new HashSet<>();
    private PrivateKey privateKey;
    private Map<Host, PublicKey> publicKeys;

    private List<Host> replicas;
    private Host self;
    private Host primary;

    private long seqNo = 0;
    private int f = 1;
    private long currentView = 0;

    public PBFTAgreement(GenericProtocol parent) {
        this.parent = parent;
    }

    private boolean amPrimary() {
        return primary.equals(self);
    }

    public void init(List<Host> replicas, Host self, Host primary, PrivateKey privateKey,
            Map<Host, PublicKey> publicKeys) throws HandlerRegistrationException {

        this.replicas = replicas;
        this.f = (replicas.size() - 1) / 3;
        this.self = self;
        this.primary = primary;
        this.privateKey = privateKey;
        this.publicKeys = publicKeys;

        ((SMReplicationProtocol) parent).registerPBFTHandler(PrePrepareMessage.MESSAGE_ID,
                (msg, from, srcProto, channel) -> onPrePrepare((PrePrepareMessage) msg, from));
        ((SMReplicationProtocol) parent).registerPBFTHandler(PrepareMessage.MESSAGE_ID,
                (msg, from, srcProto, channel) -> onPrepare((PrepareMessage) msg, from));
        ((SMReplicationProtocol) parent).registerPBFTHandler(CommitMessage.MESSAGE_ID,
                (msg, from, srcProto, channel) -> onCommit((CommitMessage) msg, from));
        ((SMReplicationProtocol) parent).registerPBFTHandler(ViewChangeMessage.MESSAGE_ID,
                (msg, from, srcProto, channel) -> onViewChange((ViewChangeMessage) msg, from));
        ((SMReplicationProtocol) parent).registerPBFTHandler(NewViewMessage.MESSAGE_ID,
                (msg, from, srcProto, channel) -> onNewView((NewViewMessage) msg, from));

    }

    public void propose(AppendRequest req) {
        if (!amPrimary()) {
            return;
        }

        long seq = ++seqNo;
        byte[] digest = SignaturesHelper.digest(req.getClientRequest());
        PBFTLogEntry entry = new PBFTLogEntry(seq, currentView, req, digest);
        log.put(seq, entry);

        PrePrepareMessage ppm = new PrePrepareMessage(self, currentView, seq, digest, req);
        broadcast(ppm);
    }

    private void broadcast(ProtoMessage msg) {
        for (Host h : replicas) {
            if (!h.equals(self)) {
                ((SMReplicationProtocol) parent).sendToReplica(msg, h);
            }
        }
    }

    private void onPrePrepare(PrePrepareMessage msg, Host from) {

        onMessageFromPrimary();

        byte[] digest = SignaturesHelper.digest(msg.getRequest().getClientRequest());
        if (!Arrays.equals(digest, msg.getDigest())) {
            return;
        }

        PBFTLogEntry entry = new PBFTLogEntry(msg.getSeq(), msg.getView(), msg.getRequest(), digest);
        log.put(msg.getSeq(), entry);

        PrepareMessage prepare = new PrepareMessage(msg.getView(), msg.getSeq(), digest, self);
        broadcast(prepare);
    }

    private void onPrepare(PrepareMessage msg, Host from) {

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            return;
        }

        entry.addPrepare(from);
        if (entry.getPreparesCount() > 2 * f) {
            CommitMessage cm = new CommitMessage(msg.getView(), msg.getSeq(), entry.getDigest(), self);
            broadcast(cm);
        }
    }

    private void onCommit(CommitMessage msg, Host from) {

        PBFTLogEntry entry = log.get(msg.getSeq());
        if (entry == null) {
            return;
        }

        entry.addCommit(from);
        if (entry.getCommitsCount() >= 2 * f + 1 && !entry.isDelivered()) {
            entry.markDelivered();
            ((SMReplicationProtocol) parent).deliverEntry(entry.getRequest(), entry.getSeq());
        }
    }

    public void onViewChange(ViewChangeMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            if (pub == null || !SignaturesHelper.checkSignature(longToBytes(msg.getNovaView()), msg.getSign(), pub)) {
                return;
            }
        } catch (Exception e) {
            return;
        }

        long v = msg.getNovaView();

        receivedViewChanges.computeIfAbsent(v, k -> new ArrayList<>()).add(msg);
        viewChangeVotes.computeIfAbsent(v, k -> new HashSet<>()).add(from);

        Set<Host> votes = viewChangeVotes.get(v);

        if (!amPrimary() && votes.size() >= f + 1 && !sentViewChange.contains(v)) {
            startViewChange(v);
            sentViewChange.add(v);
        }

        if (votes.size() >= (2 * f + 1)) {

            if (v > currentView) {
                currentView = v;
            }

            primary = replicas.get((int) (currentView % replicas.size()));
            if (amPrimary() && !startedViews.contains(v)) {
                startedViews.add(v);
                startNewView(v);
            }
        }
    }

    private void startNewView(long v) {

        List<ViewChangeMessage> vChanges = receivedViewChanges.get(v);
        List<PrePrepareMessage> prePrepares = collectPendingPrePrepares(v);

        byte[] payload = computeNewViewDigest(v, vChanges, prePrepares);

        byte[] sign = null;
        try {
            sign = SignaturesHelper.generateSignature(payload, privateKey);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao assinar a mensagem", e);
        }

        NewViewMessage newViewMsg = new NewViewMessage(v, vChanges, prePrepares, sign);

        broadcast(newViewMsg);

        if (amPrimary()) {
            for (PBFTLogEntry entry : log.values()) {
                if (!entry.isDelivered()) {
                    propose(entry.getRequest());
                }
            }
        }

    }

    public void onNewView(NewViewMessage msg, Host from) {

        try {
            PublicKey pub = publicKeys.get(from);
            if (pub == null || !SignaturesHelper.checkSignature(
                    computeNewViewDigest(msg.getNovaView(), msg.getViewChanges(), msg.getPrePreparedChanges()),
                    msg.getSign(), pub)) {
                return;
            }
        } catch (Exception e) {
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

        if (msg.getNovaView() > currentView) {
            currentView = msg.getNovaView();
            primary = replicas.get((int) (currentView % replicas.size()));
        }
    }

    private void startViewChange(long v) {

        currentView = v;
        primary = replicas.get((int) (currentView % replicas.size()));

        byte[] payload = longToBytes(v);
        byte[] sign = null;
        try {
            sign = SignaturesHelper.generateSignature(payload, privateKey);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao assinar a mensagem", e);
        }

        ViewChangeMessage vcm = new ViewChangeMessage(v, self, sign);

        broadcast(vcm);

        if (amPrimary()) {
            long maxSeq = log.keySet().stream().mapToLong(Long::longValue).max().orElse(seqNo);
            seqNo = maxSeq;
            for (PBFTLogEntry entry : log.values()) {
                if (!entry.isDelivered()) {
                    propose(entry.getRequest());
                }
            }
        }
    }

    public void onMessageFromPrimary() {
        lastPrimaryResponseTime = System.currentTimeMillis();
    }

    public void checkPrimaryTimeout() {
        if (!amPrimary() && System.currentTimeMillis() - lastPrimaryResponseTime > PRIMARY_TIMEOUT) {
            startViewChange(currentView + 1);
        }
    }

    private byte[] computeNewViewDigest(long v, List<ViewChangeMessage> vChanges, List<PrePrepareMessage> prePrepares) {
        List<byte[]> parts = new ArrayList<>();
        parts.add(longToBytes(v));
        for (ViewChangeMessage vc : vChanges) {
            if (vc.getSign() != null) {
                parts.add(vc.getSign());
            }
        }
        for (PrePrepareMessage ppm : prePrepares) {
            parts.add(ppm.getDigest());
        }
        return SignaturesHelper.digest(concat(parts));
    }

    private byte[] longToBytes(long x) {
        return new byte[] {
                (byte) (x >> 56),
                (byte) (x >> 48),
                (byte) (x >> 40),
                (byte) (x >> 32),
                (byte) (x >> 24),
                (byte) (x >> 16),
                (byte) (x >> 8),
                (byte) (x)
        };
    }

    private byte[] concat(List<byte[]> arrays) {
        int totalLength = arrays.stream().mapToInt(a -> a.length).sum();
        byte[] result = new byte[totalLength];
        int pos = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, result, pos, a.length);
            pos += a.length;
        }
        return result;
    }

    private List<PrePrepareMessage> collectPendingPrePrepares(long newView) {
        List<PrePrepareMessage> pending = new ArrayList<>();
        for (PBFTLogEntry e : log.values()) {
            if (!e.isDelivered() && e.getView() <= newView) {
                // cria um PrePrepare a partir do entry
                PrePrepareMessage ppm = new PrePrepareMessage(self, e.getView(), e.getSeq(), e.getDigest(),
                        e.getRequest());
                pending.add(ppm);
            }
        }
        return pending;
    }

}
