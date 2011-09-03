package com.spaceprogram.simplejpa.query;

import com.spaceprogram.simplejpa.EntityManagerFactoryImpl;
import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.spaceprogram.simplejpa.MyTestObject;
import com.spaceprogram.simplejpa.util.AmazonSimpleDBUtil;
import org.junit.Test;
import org.unitils.UnitilsJUnit4;
import org.unitils.mock.Mock;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Kerry Wright
 */
public class SimpleDBQueryTests extends UnitilsJUnit4 {
    Mock<EntityManagerSimpleJPA> mockEM;
    Mock<EntityManagerFactoryImpl> mockEMFactory;

    @Test
    public void testNoParameterReplacement() {
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject");
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        assertEquals("select count(*) from `simplddbquerytests-MyTestObject`", aq.getValue());
    }

    @Test
    public void testSimpleParameterReplacement() {
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject where col1 = :val1");
        q.setParameter("val1", "value1");
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        assertEquals("select count(*) from `simplddbquerytests-MyTestObject` where col1 = 'value1'", aq.getValue());
    }

    @Test
    public void testIntegerValueReplacement() {
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject where col1 = :val1");
        q.setParameter("val1", 10);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        assertEquals("select count(*) from `simplddbquerytests-MyTestObject` where col1 = '09223372036854775818'", aq.getValue());
    }

    @Test
    public void testDateValueReplacement() {
        Date now = new Date();
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject where col1 = :val1");
        q.setParameter("val1", now);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        assertEquals("select count(*) from `simplddbquerytests-MyTestObject` where col1 = '"+ AmazonSimpleDBUtil.encodeDate(now)+"'", aq.getValue());
    }

    @Test
    public void testMultipleReplacements() {
        Date now = new Date();
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select * from MyTestObject where col1 = :val1 and col10 > :val10");
        q.setParameter("val1", now);
        q.setParameter("val10", 10);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertFalse(aq.isCount());
        String expected = new StringBuilder().append("select * from `simplddbquerytests-MyTestObject` where col1 = '")
                .append(AmazonSimpleDBUtil.encodeDate(now))
                .append("' and col10 > '09223372036854775818'").toString();
        assertEquals(expected, aq.getValue());
    }

    @Test
    public void testCollectionValueReplacement() {
        Date now = new Date();
        List<?> param = Arrays.asList(now, 10);
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject where col1 in (:val1)");
        q.setParameter("val1", param);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        String convertedDate = AmazonSimpleDBUtil.encodeDate(now);
        String convertedNum = "09223372036854775818";
        assertEquals(
                String.format("select count(*) from `simplddbquerytests-MyTestObject` where col1 in ('%s', '%s')", convertedDate, convertedNum),
                aq.getValue());
    }

    @Test
    public void testArrayValueReplacement() {
        Date now = new Date();
        Object[] param = new Object[]{now, 10};
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select count(*) from MyTestObject where col1 in (:val1)");
        q.setParameter("val1", param);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertTrue(aq.isCount());
        String convertedDate = AmazonSimpleDBUtil.encodeDate(now);
        String convertedNum = "09223372036854775818";
        assertEquals(
                String.format("select count(*) from `simplddbquerytests-MyTestObject` where col1 in ('%s', '%s')", convertedDate, convertedNum),
                aq.getValue());
    }

    @Test
    public void testPositionalParameterReplacement() {
        Date now = new Date();
        mockEM.returns(mockEMFactory.getMock()).getFactory();
        mockEM.returns(MyTestObject.class).ensureClassIsEntity("MyTestObject");
        mockEMFactory.returns("simplddbquerytests").getPersistenceUnitName();

        SimpleDBQuery q = new SimpleDBQuery(mockEM.getMock(), "select * from MyTestObject where col1 = ?1 and col10 > ?2");
        q.setParameter(1, now);
        q.setParameter(2, 10);
        AmazonQueryString aq = q.createAmazonQuery(false);
        assertFalse(aq.isCount());
        String expected = new StringBuilder().append("select * from `simplddbquerytests-MyTestObject` where col1 = '")
                .append(AmazonSimpleDBUtil.encodeDate(now))
                .append("' and col10 > '09223372036854775818'").toString();
        assertEquals(expected, aq.getValue());
    }

    @Test
    public void testConvertToCountQuery() {
        AmazonQueryString aq = new AmazonQueryString("select * from DomainClass", false);
        assertEquals("select count(*) from DomainClass", SimpleDBQuery.convertToCountQuery(aq));
    }

    @Test
    public void testConvertCountToCountQuery() {
        AmazonQueryString aq = new AmazonQueryString("select count(*) from DomainClass", true);
        assertEquals("select count(*) from DomainClass", SimpleDBQuery.convertToCountQuery(aq));
    }

    @Test
    public void testConvertFieldQueryToCount() {
        AmazonQueryString aq = new AmazonQueryString("select itemName(), col1, col2 from DomainClass", true);
        assertEquals("select count(*) from DomainClass", SimpleDBQuery.convertToCountQuery(aq));
    }

    @Test
    public void testExtractClassFromQuery() {
        assertEquals(MyTestObject.class.getName(), SimpleDBQuery.extractClassFromQuery("select * from com.spaceprogram.simplejpa.MyTestObject"));
        assertEquals(MyTestObject.class.getName(), SimpleDBQuery.extractClassFromQuery("select count(*) from com.spaceprogram.simplejpa.MyTestObject"));
        assertEquals(MyTestObject.class.getSimpleName(), SimpleDBQuery.extractClassFromQuery("select * from MyTestObject where 1 = 1"));
    }
}
