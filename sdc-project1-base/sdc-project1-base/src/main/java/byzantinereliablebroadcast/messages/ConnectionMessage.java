package byzantinereliablebroadcast.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ConnectionMessage extends ProtoMessage {


    public final static short MESSAGE_ID = 1006;

    private final Host myself;


    public ConnectionMessage(Host me){
        super(MESSAGE_ID);
        this.myself = me;
    }



    public Host getSender() {
        return myself;
    }


}



