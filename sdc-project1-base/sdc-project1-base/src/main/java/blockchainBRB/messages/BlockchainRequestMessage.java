package blockchainBRB.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class BlockchainRequestMessage extends SignedProtoMessage {

	public final static short MESSAGE_ID = 301;

	private final Host myself;


	public BlockchainRequestMessage(Host me) {
		super(MESSAGE_ID);
		this.myself = me;
	}

	public Host getSender() {
		return myself;
	}

	public static final SignedMessageSerializer<BlockchainRequestMessage> serializer = new SignedMessageSerializer<BlockchainRequestMessage>() {

		@Override
		public void serializeBody(BlockchainRequestMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.myself, out);
		}

		@Override
		public BlockchainRequestMessage deserializeBody(ByteBuf in) throws IOException {
			Host h = Host.serializer.deserialize(in);
			
			byte[] challengeSigned = new byte[in.readInt()];
			
			if(challengeSigned.length > 0) {
				in.readBytes(challengeSigned);
			}
			
			return new BlockchainRequestMessage(h);
		}
		
	};
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return BlockchainRequestMessage.serializer;
	}

}
