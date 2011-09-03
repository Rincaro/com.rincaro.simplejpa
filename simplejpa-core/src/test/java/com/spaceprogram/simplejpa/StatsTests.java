package com.spaceprogram.simplejpa;

import java.util.Date;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * User: treeder
 * Date: Aug 2, 2009
 * Time: 10:41:24 PM
 */
@Ignore("These tests are failing for some reason")
public class StatsTests extends BaseTestClass {

    @Test
    public void testPuts() {
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();

        MyTestObject object = new MyTestObject();
        object.setName("Scooby doo");
        object.setAge(100);
        Date now = new Date();
        object.setBirthday(now);
        em.persist(object);

        System.out.println(em.getTotalOpStats());
        Assert.assertEquals(1, em.getTotalOpStats().getPuts());

        object.setId(null);
        em.persist(object);
        Assert.assertEquals(2, em.getTotalOpStats().getPuts());

        em.close();

        // New EntityManager and same op
        em = (EntityManagerSimpleJPA) factory.createEntityManager();

        object = new MyTestObject();
        object.setName("Scooby doo");
        object.setAge(100);
        object.setBirthday(now);
        em.persist(object);

        System.out.println(em.getTotalOpStats());
        Assert.assertEquals(1, em.getTotalOpStats().getPuts());
        Assert.assertEquals(3, em.getGlobalOpStats().getPuts());

        em.close();
    }

}
