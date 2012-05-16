package com.rincaro.simplejpa.cache;


/**
 * 
 * 
 * Wrapper for KittyCache: http://code.google.com/p/kitty-cache/
 * 
 */
public class KittyCacheWrapper extends KittyCache implements Cache {

    public KittyCacheWrapper(int i) {
        super(i);
    }

    @Override
    public Object getObj(Object o) {
        return get(o);
    }

    @Override
    public void put(Object o, Object o1) {
        put(o,o1, 10000);       
    }
}
