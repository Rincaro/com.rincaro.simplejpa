package com.rincaro.simplejpa.model;

import org.joda.time.DateTime;

import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import java.util.Date;

/**
 * User: treeder
 * Date: Jun 2, 2008
 * Time: 11:22:45 AM
 */
public class JodaDateTimedEntityListener {

    @PrePersist
    public void prePersist(Object object) {
        if(object instanceof JodaDateTimed){
            JodaDateTimed timestamped = (JodaDateTimed) object;
            DateTime now = new DateTime();
            timestamped.setCreated(now);
            timestamped.setUpdated(now);
        }
    }

    @PreUpdate
    public void preUpdate(Object object) {
        if(object instanceof JodaDateTimed){
            JodaDateTimed timestamped = (JodaDateTimed) object;
            timestamped.setUpdated(new DateTime());
        }
    }
}
