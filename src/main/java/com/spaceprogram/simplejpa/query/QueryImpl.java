package com.spaceprogram.simplejpa.query;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import com.amazonaws.AmazonClientException;
import com.spaceprogram.simplejpa.AnnotationInfo;
import com.spaceprogram.simplejpa.EntityManagerFactoryImpl;
import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.spaceprogram.simplejpa.NamingHelper;
import com.spaceprogram.simplejpa.PersistentProperty;

/**
 * Need to support the following: <p/> <p/> - Navigation operator (.) DONE - Arithmetic operators: +, - unary *, / multiplication and division +, - addition and subtraction -
 * Comparison operators : =, >, >=, <, <=, <> (not equal), [NOT] BETWEEN, [NOT] LIKE, [NOT] IN, IS [NOT] NULL, IS [NOT] EMPTY, [NOT] MEMBER [OF] - Logical operators: NOT AND OR
 * <p/> see: http://docs.solarmetric.com/full/html/ejb3_langref.html#ejb3_langref_where <p/> User: treeder Date: Feb 8, 2008 Time: 7:33:20 PM
 */
public class QueryImpl extends AbstractQuery {

    private static Logger logger = Logger.getLogger(QueryImpl.class.getName());

    public static List<String> tokenizeWhere(String where) {
        List<String> split = new ArrayList<String>();
        Pattern pattern = Pattern.compile(conditionRegex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(where);
        int lastIndex = 0;
        String s;
        int i = 0;
        while (matcher.find()) {
            s = where.substring(lastIndex, matcher.start()).trim();
            logger.finest("value: " + s);
            split.add(s);
            s = matcher.group();
            split.add(s);
            logger.finest("matcher found: " + s + " at " + matcher.start() + " to " + matcher.end());
            lastIndex = matcher.end();
            i++;
        }
        s = where.substring(lastIndex).trim();
        logger.finest("final:" + s);
        split.add(s);
        return split;
    }

    private JPAQuery q;

    public static String conditionRegex = "(<>)|(>=)|(<=)|=|>|<|\\band\\b|\\bor\\b|\\bis\\b|\\blike\\b|\\bin\\b";
    private String qString;

    // private AmazonQueryString amazonQuery;
    private Map<String, List<String>> foreignIds = new HashMap();

    public QueryImpl(EntityManagerSimpleJPA em, JPAQuery q) {
        super(em);
        this.q = q;
        this.qString = q.toString();
        this.init(em);
    }

    public QueryImpl(EntityManagerSimpleJPA em, String qString) {
        super(em);
        this.qString = qString;
        logger.fine("query=" + qString);
        this.q = new JPAQuery();
        JPAQueryParser parser = new JPAQueryParser(this.q, qString);
        parser.parse();
        this.init(em);
    }

    private Boolean appendCondition(Class tClass, StringBuilder sb, String field, String comparator, String param) {
        comparator = comparator.toLowerCase();
        AnnotationInfo ai = this.em.getAnnotationManager().getAnnotationInfo(tClass);

        String fieldSplit[] = field.split("\\.");
        if (fieldSplit.length == 1) {
            field = fieldSplit[0];
            // System.out.println("split: " + field + " param=" + param);
            if (field.equals(param)) {
                return false;
            }
        } else if (fieldSplit.length == 2) {
            field = fieldSplit[1];
        } else if (fieldSplit.length == 3) {
            // NOTE: ONLY SUPPORTING SECOND LEVEL OF GRAPH RIGHT NOW
            // then we have to reach down the graph here. eg: myOb.ob2.name or myOb.ob2.id
            // if filtering by id, then don't need to query for second object, just add a filter on the id field
            String refObjectField = fieldSplit[1];
            field = fieldSplit[2];
            // System.out.println("field=" + field);
            Class refType = ai.getPersistentProperty(refObjectField).getPropertyClass();
            AnnotationInfo refAi = this.em.getAnnotationManager().getAnnotationInfo(refType);
            PersistentProperty getterForField = refAi.getPersistentProperty(field);
            // System.out.println("getter=" + getterForField);
            String paramValue = this.getParamValueAsStringForAmazonQuery(param, getterForField);
            logger.finest("paramValue=" + paramValue);
            String idFieldName = refAi.getIdMethod().getFieldName();
            if (idFieldName.equals(field)) {
                logger.finer("Querying using id field, no second query required.");
                this.appendFilter(sb, NamingHelper.foreignKey(refObjectField), comparator, paramValue);
            } else {
                // no id method, so query for other object(s) first, then apply the returned value to the original query.
                // todo: this needs some work (multiple ref objects? multiple params on same ref object?)
                List<String> ids = this.foreignIds.get(field);
                // System.out.println("got foreign ids=" + ids);
                if (ids == null) {
                    Query sub = this.em.createQuery("select o from " + refType.getName() + " o where o." + field + " " + comparator + " :paramValue");
                    sub.setParameter("paramValue", this.parameters.get(this.paramName(param)));
                    List subResults = sub.getResultList();
                    ids = new ArrayList<String>();
                    for (Object subResult : subResults) {
                        ids.add(this.em.getId(subResult));
                    }
                    this.foreignIds.put(field, ids); // Store the ids for next use, really reduces queries when using this repetitively
                }
                if (ids.size() > 0) {
                    this.appendIn(sb, NamingHelper.foreignKey(refObjectField), ids);
                } else {
                    // no matches so should return nothing right? only if an AND query I guess
                    return null;
                }
            }
            return true;
        } else {
            throw new PersistenceException("Invalid field used in query: " + field);
        }
        logger.finest("field=" + field);
        // System.out.println("field=" + field + " paramValue=" + param);
        PersistentProperty getterForField = ai.getPersistentProperty(field);
        if (getterForField == null) {
            throw new PersistenceException("No getter for field: " + field);
        }
        String columnName = getterForField.getColumnName();
        if (columnName == null) {
            columnName = field;
        }
        if (comparator.equals("is")) {
            if (param.equalsIgnoreCase("null")) {
                sb.append(columnName).append(" is null");
                // appendFilter(sb, true, columnName, "starts-with", "");
            } else if (param.equalsIgnoreCase("not null")) {
                sb.append(columnName).append(" is not null");
                // appendFilter(sb, false, columnName, "starts-with", "");
            } else {
                throw new PersistenceException("Must use only 'is null' or 'is not null' with where condition containing 'is'");
            }
        } else if (comparator.equals("like")) {
            comparator = "like";
            String paramValue = this.getParamValueAsStringForAmazonQuery(param, getterForField);
            // System.out.println("param=" + paramValue + "___");
            // paramValue = paramValue.endsWith("%") ? paramValue.substring(0, paramValue.length() - 1) : paramValue;
            // System.out.println("param=" + paramValue + "___");
            // param = param.startsWith("%") ? param.substring(1) : param;
            this.appendFilter(sb, columnName, comparator, paramValue);
        } else if (comparator.equals("in")) {
            comparator = "in";
            // System.out.println("param=" + paramValue + "___");
            // paramValue = paramValue.endsWith("%") ? paramValue.substring(0, paramValue.length() - 1) : paramValue;
            // System.out.println("param=" + paramValue + "___");
            // param = param.startsWith("%") ? param.substring(1) : param;
            List<String> list = new ArrayList<String>();
            Object value = getParameters().get(paramName(param));
            if (value.getClass().isArray()) {
                list.addAll(Arrays.asList((String[])value));
            } else {
                list.addAll((Collection<String>) value);
            }
            this.appendIn(sb, columnName, list);
        } else {
            // Handle the translation of NOT EQUALS
            if ("<>".equals(comparator)) {
                comparator = "!=";
            }

            String paramValue = this.getParamValueAsStringForAmazonQuery(param, getterForField);
            logger.finer("paramValue=" + paramValue);
            logger.finer("comp=[" + comparator + "]");
            this.appendFilter(sb, columnName, comparator, paramValue);
        }
        return true;
    }

    private void appendFilter(StringBuilder sb, boolean not, String field, String comparator, String param, boolean quoteParam) {
        if (not) {
            sb.append("not ");
        }
        boolean quoteField = !NamingHelper.NAME_FIELD_REF.equals(field);
        if (quoteField) {
            sb.append("`");
        }
        sb.append(field);
        if (quoteField) {
            sb.append("`");
        }
        sb.append(" ");
        sb.append(comparator);
        sb.append(" ");
        if (quoteParam) {
            sb.append("'");
        }
        sb.append(param);
        if (quoteParam) {
            sb.append("'");
        }
    }

    private void appendFilter(StringBuilder sb, String field, String comparator, String param) {
        this.appendFilter(sb, false, field, comparator, param, false);
    }

    /*
     * public StringBuilder toAmazonQuery(){ return toAmazonQuery( }
     */

    private void appendFilterMultiple(StringBuilder sb, String field, String comparator, List params) {
        int count = 0;
        for (Object param : params) {
            if (count > 0) {
                sb.append(" and ");
            }
            sb.append(field);
            sb.append(comparator).append(" '").append(param).append("'");
            count++;
        }
    }

    private void appendIn(StringBuilder sb, String field, List<String> params) {
        sb.append("`").append(field).append("`");
        sb.append(" ");
        sb.append("IN");
        sb.append(" (");
        for (int i = 0; i < params.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("'").append(params.get(i)).append("'");
        }
        sb.append(")");
    }

    @Override
    public AmazonQueryString createAmazonQuery(boolean appendLimit) throws NoResultsException, AmazonClientException {
        String select = this.q.getResult();
        boolean count = false;
        if (select != null && select.contains("count")) {
            // System.out.println("HAS COUNT: " + select);
            count = true;
        }
        AnnotationInfo ai = this.em.getAnnotationManager().getAnnotationInfo(this.tClass);

        // Make sure querying the root Entity class
        String domainName = this.em.getDomainName(ai.getRootClass());
        if (domainName == null) {
            return null;
            // throw new NoResultsException();
        }
        StringBuilder amazonQuery;
        if (this.q.getFilter() != null) {
            amazonQuery = this.toAmazonQuery(this.tClass, this.q);
            if (amazonQuery == null) {
                // throw new NoResultsException();
                return null;
            }
        } else {
            amazonQuery = new StringBuilder();
        }
        if (ai.getDiscriminatorValue() != null) {
            if (amazonQuery.length() == 0) {
                amazonQuery = new StringBuilder();
            } else {
                amazonQuery.append(" and ");
            }
            this.appendFilter(amazonQuery, EntityManagerFactoryImpl.DTYPE, "=", "'" + ai.getDiscriminatorValue() + "'");
        }

        // now for sorting
        String orderBy = this.q.getOrdering();
        if (orderBy != null && orderBy.length() > 0) {
            // amazonQuery.append(" sort ");
            amazonQuery.append(" order by ");
            String orderByOrder = "asc";
            String orderBySplit[] = orderBy.split(" ");
            if (orderBySplit.length > 2) {
                throw new PersistenceException("Can only sort on a single attribute in SimpleDB. Your order by is: " + orderBy);
            }
            if (orderBySplit.length == 2) {
                orderByOrder = orderBySplit[1];
            }
            String orderByAttribute = orderBySplit[0];
            String fieldSplit[] = orderByAttribute.split("\\.");
            if (fieldSplit.length == 1) {
                orderByAttribute = fieldSplit[0];
            } else if (fieldSplit.length == 2) {
                orderByAttribute = fieldSplit[1];
            }
            // amazonQuery.append("'");
            amazonQuery.append(orderByAttribute);
            // amazonQuery.append("'");
            amazonQuery.append(" ").append(orderByOrder);
        }
        StringBuilder fullQuery = new StringBuilder();
        fullQuery.append("select ");
        fullQuery.append(count ? "count(*)" : "*");
        fullQuery.append(" from `").append(domainName).append("` ");
        if (amazonQuery.length() > 0) {
            fullQuery.append("where ");
            fullQuery.append(amazonQuery);
        }
        String logString = "amazonQuery: Domain=" + domainName + ", query=" + fullQuery;
        logger.fine(logString);
        if (this.em.getFactory().isPrintQueries()) {
            System.out.println(logString);
        }

        if (!count && appendLimit && this.maxResults >= 0) {
            fullQuery.append(" limit ").append(Math.min(MAX_RESULTS_PER_REQUEST, this.maxResults));
        }
        return new AmazonQueryString(fullQuery.toString(), count);
    }

    public Map<String, List<String>> getForeignIds() {
        return this.foreignIds;
    }

    public JPAQuery getQ() {
        return this.q;
    }

    public String getQString() {
        return this.qString;
    }

    private void init(EntityManagerSimpleJPA em) {

        String from = this.q.getFrom();
        logger.finer("from=" + from);
        logger.finer("where=" + this.q.getFilter());
        if (this.q.getOrdering() != null && this.q.getFilter() == null) {
            throw new PersistenceException("Attribute in ORDER BY [" + this.q.getOrdering() + "] must be included in a WHERE filter.");
        }

        String split[] = this.q.getFrom().split(" ");
        String obClass = split[0];
        this.tClass = em.ensureClassIsEntity(obClass);
        this.consistentRead = em.isConsistentRead();
    }

    public void setForeignIds(Map<String, List<String>> foreignIds) {
        this.foreignIds = foreignIds;
    }

    @Override
    public int getCount() {
        try {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Getting size.");
            }

            JPAQuery queryClone = (JPAQuery) this.getQ().clone();
            queryClone.setResult("count(*)");
            QueryImpl query2 = new QueryImpl(this.em, queryClone);
            query2.setParameters(this.getParameters(), this.getPositionalParameters());
            query2.setForeignIds(this.getForeignIds());

            List results = query2.getResultList();
            int resultCount = ((Long) results.get(0)).intValue();

            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Got:" + resultCount);
            }

            if (this.maxResults >= 0 && resultCount > this.maxResults) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer("Too much, adjusting to maxResults: " + this.maxResults);
                }

                return this.maxResults;
            } else {
                return resultCount;
            }
        } catch (CloneNotSupportedException e) {
            throw new PersistenceException(e);
        }
    }

    /*
     * public AmazonQueryString getAmazonQuery() { return amazonQuery; } public void setAmazonQuery(AmazonQueryString amazonQuery) { this.amazonQuery = amazonQuery; }
     */
    public void setQ(JPAQuery q) {
        this.q = q;
    }

    public void setQString(String qString) {
        this.qString = qString;
    }

    public StringBuilder toAmazonQuery(Class tClass, JPAQuery q) {
        StringBuilder sb = new StringBuilder();
        String where = q.getFilter();
        where = where.trim();
        // now split it into pieces
        List<String> whereTokens = tokenizeWhere(where);
        Boolean aok = false;
        for (int i = 0; i < whereTokens.size();) {
            if (aok && i > 0) {
                String andOr = whereTokens.get(i);
                if (andOr.equalsIgnoreCase("OR")) {
                    sb.append(" or ");
                } else {
                    sb.append(" and ");
                }
            }
            if (i > 0) {
                i++;
            }
            // System.out.println("sbbefore=" + sb);
            // special null cases: is null and is not null
            String firstParam = whereTokens.get(i);
            i++;
            String secondParam = whereTokens.get(i);
            i++;
            String thirdParam = whereTokens.get(i);
            if (thirdParam.equalsIgnoreCase("not")) {
                i++;
                thirdParam += " " + whereTokens.get(i);
            }
            i++;
            aok = this.appendCondition(tClass, sb, firstParam, secondParam, thirdParam);
            // System.out.println("sbafter=" + sb);
            if (aok == null) {
                return null; // todo: only return null if it's an AND query, or's should still continue, but skip the intersection part
            }
        }

        logger.fine("query=" + sb);
        return sb;
    }

    @Override
    public String toString() {
        return "QueryImpl{" + "em=" + this.em + ", q=" + this.q + ", parameters=" + this.parameters + ", maxResults=" + this.maxResults + ", qString='" + this.qString + '\'' + '}';
    }

}
