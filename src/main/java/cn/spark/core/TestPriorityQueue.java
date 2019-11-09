package cn.spark.core;

import java.util.Comparator;
import java.util.PriorityQueue;

public class TestPriorityQueue {

    public static void main(String[] args) {
        PriorityQueue<Integer> queue = new PriorityQueue<>();
        queue.add(56);
        queue.add(88);
        queue.add(87);
        queue.add(67);
        queue.add(77);
        int s = queue.size();
        System.out.println(queue);
        for(int i=0;i<s;i++){
            System.out.println(queue.poll());
        }

    }
}
