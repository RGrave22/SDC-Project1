package blockchainReplication.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ViewChangeTimer extends ProtoTimer {

    public static final short TIMER_ID = 1006;

    public ViewChangeTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
