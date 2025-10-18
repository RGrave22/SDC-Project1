package PBFT.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class NewViewNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 1008;
    private final long newView;

    public NewViewNotification(long newView) {
        super(NOTIFICATION_ID);
        this.newView = newView;
    }

    public long getNewView() {
        return newView;
    }
}
