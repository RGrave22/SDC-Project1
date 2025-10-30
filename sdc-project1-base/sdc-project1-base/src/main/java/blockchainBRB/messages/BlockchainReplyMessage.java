package blockchainBRB.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class BlockchainReplyMessage extends SignedProtoMessage {

	public final static short MESSAGE_ID = 302;

	private final Host myself;
	private final byte[] blockchainReply;


	public BlockchainReplyMessage(Host me, byte[] blockchainReply) {
		super(MESSAGE_ID);
		this.myself = me;
		this.blockchainReply = blockchainReply;
	}

	public byte[] getBlockchainReply() {
		return blockchainReply;
	}

	public Host getSender() {
		return myself;
	}

	public static final SignedMessageSerializer<BlockchainReplyMessage> serializer = new SignedMessageSerializer<BlockchainReplyMessage>() {

		@Override
		public void serializeBody(BlockchainReplyMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.myself, out);
			
			if(m.blockchainReply != null) {
				out.writeInt(m.blockchainReply.length);
				out.writeBytes(m.blockchainReply);
			}
			
		}

		@Override
		public BlockchainReplyMessage deserializeBody(ByteBuf in) throws IOException {
			Host h = Host.serializer.deserialize(in);
			
			byte[] challengeSigned = new byte[in.readInt()];
			
			if(challengeSigned.length > 0) {
				in.readBytes(challengeSigned);
			}
			
			return new BlockchainReplyMessage(h, challengeSigned);
		}
		
	};
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return BlockchainReplyMessage.serializer;
	}

}
