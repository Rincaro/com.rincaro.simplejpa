package com.spaceprogram.simplejpa.model;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.DateTime;

import javax.persistence.*;
import java.io.Serializable;

/**
 * Can use as a common base class.
 *
 * User: treeder
 * Date: Feb 16, 2008
 * Time: 4:44:05 PM
 */
@MappedSuperclass
@EntityListeners(JodaDateTimedEntityListener.class)
public class IdedJodaDateTimedBase extends IdedBase implements Ided, JodaDateTimed, Serializable {
    protected String id;
    private DateTime created;
    private DateTime updated;

    @Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	public String getId() {
		return id;
	}

    public void setId(String id) {
        this.id = id;
    }

    public DateTime getCreated() {
        return created;
    }

    public void setCreated(DateTime created) {
        this.created = created;
    }

    public DateTime getUpdated() {
        return updated;
    }

    public void setUpdated(DateTime now) {
        this.updated = now;
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("created", created)
                .append("updated", updated)
                .toString();
    }
}
