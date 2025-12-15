package org.anarplex.lib.nntp.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateAndTime {

    public static final String NNTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss Z"; // the standard NNTP date format

    // Returns the current date and time in the standard NNTP date format
    public static String now() {
        SimpleDateFormat sdf = new SimpleDateFormat(NNTP_DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }
}
