package com.westar.local;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Java堆结构PriorityQueue完全解析: 参考https://www.cnblogs.com/tstd/p/5125949.html
 */
public class PriorityQueueTest {
    public static void main(String[] args) {
        PriorityQueue<Long>   priorityQueue = new PriorityQueue<>(10, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                //大的在前面
                return o2.compareTo(o1);
            }
        });
        //添加数据
        priorityQueue.offer(100L);
        priorityQueue.offer(23L);
        priorityQueue.offer(44L);
        priorityQueue.offer(55L);
        priorityQueue.offer(12L);
        priorityQueue.offer(66L);
        priorityQueue.offer(3L);
        priorityQueue.offer(5L);
        priorityQueue.offer(1L);

        System.out.println(priorityQueue.toString());

        //取得第一个数
        long firstNum = priorityQueue.peek();
        System.out.println(firstNum);
        System.out.println(priorityQueue.toString());
        //取得第一个数，并移除
        long firstValforRemove = priorityQueue.poll();
        System.out.println(firstValforRemove);
        System.out.println(priorityQueue.toString());
    }
}
