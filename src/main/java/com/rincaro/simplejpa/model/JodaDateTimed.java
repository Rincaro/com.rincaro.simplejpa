package com.rincaro.simplejpa.model;

import org.joda.time.DateTime;

/**
 * User: treeder
 * Date: Jun 2, 2008
 * Time: 11:23:57 AM
 */
public interface JodaDateTimed {
    void setCreated(DateTime d);
    DateTime getCreated();
    void setUpdated(DateTime d);
    DateTime getUpdated();
}
