package net.enset.events;

import java.util.Date;

public record PageEvent (String name, String user, Date date, long duree) {
}
