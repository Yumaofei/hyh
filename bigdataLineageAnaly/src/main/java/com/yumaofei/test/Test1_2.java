package com.yumaofei.test;


import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;

import java.lang.reflect.Array;

public class Test1_2 {
    public static void main(String[] args) {
        int i =2 ;

        int[] a = {1,2,3,4,5,6,7,8,9,10,11};
        int i1 = binarySearch(a, 12, 0, 10);
        System.out.println(i1);
    }
    public static  int  binarySearch(int[] arr, int num, int start, int end){
        //获取最中间的值
        int mid = (end-start )/2+start;
        //校验中间值是否就是目标值，不是则继续截取数组。
        if (arr[mid]==num) {
            return mid;
        }
        //校验参数合法性
        if (start>=num) {return -1;}

        if (num<arr[mid]){  //判断目标值 小于 则从起始位置到 中间位置继续递归
            return     binarySearch(arr,num,start,mid-1);
        }
        if (num>arr[mid]){ //判断目标值 大于 则从中间位置到 结束位置继续递归
            return     binarySearch(arr,num,mid+1,end);
        }

        return -1;
    }
}
