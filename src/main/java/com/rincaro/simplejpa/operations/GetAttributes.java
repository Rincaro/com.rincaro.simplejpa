package com.rincaro.simplejpa.operations;

import com.rincaro.simplejpa.EntityManagerSimpleJPA;
import com.rincaro.simplejpa.ItemAndAttributes;
import com.rincaro.simplejpa.SdbItem;

import java.util.concurrent.Callable;

/**
 * For our concurrent SimpleDB accesses.
 *
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 7:55:30 PM
 */
public class GetAttributes implements Callable<ItemAndAttributes> {
    private SdbItem item;
    private EntityManagerSimpleJPA em;

    public GetAttributes(SdbItem item, EntityManagerSimpleJPA em) {
        this.item = item;
        this.em = em;
    }

    public ItemAndAttributes call() throws Exception {
        em.getTotalOpStats().incrementGets();
        return new ItemAndAttributes(item, item.getAttributes());
    }
}

