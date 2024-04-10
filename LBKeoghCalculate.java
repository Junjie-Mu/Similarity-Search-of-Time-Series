package com.bigdata.spark.core.LBKeogh;

import java.util.HashMap;
import java.util.Map;
/**
 * @Author: JUNJIE MU
 */
public class LBKeoghCalculate {
    public static Map<Double, String[]> calculate(String[] inputData, String[] dataLake){
        Map<Double, String[]> map = new HashMap<>(2);
        //将输入数据写入
        double[] query = new double[inputData.length];
        for (int i = 0; i < inputData.length; i++) {
            query[i] = Double.parseDouble(inputData[i]);
        }
        //将比较数据数据湖写入
        double[] data = new double[dataLake.length];
        for (int j = 0; j < dataLake.length; j++) {
            data[j] = Double.parseDouble(dataLake[j]);
        }
        double LBKeoghDistance = LBKeogh(query, data, 1);
        map.put(LBKeoghDistance,dataLake);
        return map;
    }


    /**
     * @param query 输入雨量序列
     * @param data 雨量序列数据池
     * @param r 窗口大小参数
     * @return
     */
    public static double LBKeogh(double[] query, double[] data, int r) {
        int n = query.length;
        double lbSum = 0;
        //遍历计算每个元素的上下界
        for (int i = 0; i < n; i++) {
            double upperBound, lowerBound;
            if (i + r < n) {
                upperBound = getUpperBound(query, data, i, r);
                lowerBound = getLowerBound(query, data, i, r);
            } else {
                upperBound = getUpperBound(query, data, i, n - i - 1);
                lowerBound = getLowerBound(query, data, i, n - i - 1);
            }
            //计算距离部分和
            if (query[i] > upperBound) {
                lbSum += Math.pow(query[i] - upperBound, 2);
            } else if (query[i] < lowerBound) {
                lbSum += Math.pow(query[i] - lowerBound, 2);
            }
        }
        return Math.sqrt(lbSum);
    }

    /**
     * 计算包络线上界
     */
    private static double getUpperBound(double[] query, double[] data, int index, int r) {
        int n = query.length;
        int start = Math.max(0, index - r);
        int end = Math.min(n - 1, index + r);
        double upperBound = Double.NEGATIVE_INFINITY;
        if (start > data.length - 1) {
            // 处理窗口范围超出data序列的情况
            upperBound = Double.POSITIVE_INFINITY;
            return upperBound;
        }
        // 修正end索引，确保不超过data序列长度
        end = Math.min(end, data.length - 1);
        for (int i = start; i <= end; i++) {
            if (data[i] > upperBound) {
                upperBound = data[i];
            }
        }
        return upperBound;
    }

    /**
     * 计算包络线下界
     */
    private static double getLowerBound(double[] query, double[] data, int index, int r) {
        int n = query.length;
        int start = Math.max(0, index - r);
        int end = Math.min(n - 1, index + r);
        double lowerBound = Double.POSITIVE_INFINITY;
        if (start > data.length - 1) {
            // 处理窗口范围超出data序列的情况
            lowerBound = Double.NEGATIVE_INFINITY;
            return lowerBound;
        }
        end = Math.min(end, data.length - 1);
        for (int i = start; i <= end; i++) {
            if (data[i] < lowerBound) {
                lowerBound = data[i];
            }
        }
        return lowerBound;
    }
}
