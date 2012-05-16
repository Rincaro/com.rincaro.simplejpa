package com.rincaro.simplejpa.cache;

public interface Cache {
    int size();

    Object getObj(Object o);

    void put(Object o, Object o1);

    boolean remove(Object o);

    void clear();
}
