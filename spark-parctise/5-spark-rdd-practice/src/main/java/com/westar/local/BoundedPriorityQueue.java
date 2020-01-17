package com.westar.local;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.function.Consumer;

public class BoundedPriorityQueue<A> implements Iterable<A>, Serializable {

    //基于PriorityQueue来实现我们的单机版的topN
    private PriorityQueue<A> underlying = null;
    //topN中的N
    private int maxSize;
    //比较器
    private Comparator<? super A> comparator;

    public BoundedPriorityQueue(int maxSize,Comparator<? super A> comparator){
        underlying = new PriorityQueue(maxSize,comparator);
        this.maxSize = maxSize;
        this.comparator = comparator;
    }

    private int size(){
        return underlying.size();
    }

    public void addAll(Iterable<A> iterable){
        Iterator<A> iterator = iterable.iterator();
        while (iterator.hasNext()){
            add(iterator.next());
        }
    }

    public BoundedPriorityQueue<A> add(A element) {
        if(size() < maxSize){
            underlying.offer(element);
        }else{
            maybeReplaceLowest(element);
        }
        return this;
    }

    private void maybeReplaceLowest(A element) {
        A head = underlying.peek();
        if(null!=head && comparator.compare(head,element) < 0){
            underlying.poll();
            underlying.offer(element);
        }

    }
    @Override
    public Iterator<A> iterator() {
        return underlying.iterator();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }
}
