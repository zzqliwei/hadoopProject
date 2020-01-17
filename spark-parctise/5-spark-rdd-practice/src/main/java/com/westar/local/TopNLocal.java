package com.westar.local;

import java.util.Comparator;

public class TopNLocal {
    public static void main(String[] args) {
        int n = 3;
        BoundedPriorityQueue<Long> priorityQueue = new BoundedPriorityQueue(3, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                //小的在前面
                return o1.compareTo(o2);
            }
        });

        priorityQueue.add(100L);
        System.out.println(priorityQueue);
        priorityQueue.add(23L);
        System.out.println(priorityQueue);
        priorityQueue.add(44L);
        System.out.println(priorityQueue);
        priorityQueue.add(55L);

        System.out.println(priorityQueue);

        priorityQueue.add(200L);
        System.out.println(priorityQueue);

        priorityQueue.add(308L);
        System.out.println(priorityQueue);

        priorityQueue.add(8L);
        System.out.println(priorityQueue);

    }
}
