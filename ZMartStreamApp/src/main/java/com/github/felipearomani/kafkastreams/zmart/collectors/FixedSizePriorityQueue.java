package com.github.felipearomani.kafkastreams.zmart.collectors;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class FixedSizePriorityQueue<T> {
    private TreeSet<T> inner;
    private int maxSize;


    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        this.inner = new TreeSet<>(comparator);
        this.maxSize = maxSize;
    }


    public FixedSizePriorityQueue<T> add(T element) {
        inner.add(element);
        if (inner.size() > maxSize) {
            inner.pollLast();
        }
        return this;
    }

    public FixedSizePriorityQueue<T> remove(T element) {
        if (inner.contains(element)) {
            inner.remove(element);
        }
        return this;
    }

    public Iterator<T> iterator() {
        return inner.iterator();
    }

    @Override
    public String toString() {
        return "FixedSizePriorityQueue{" +
                "QueueContents=" + inner;

    }
}
