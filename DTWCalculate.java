package com.bigdata.spark.core.DTW;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: JUNJIE MU
 */
public class DTWCalculate {

    /**
     * 得到相似度最大（即距离最小的）序列
     */
    public static Map<Double, String[]> calculate(String[] inputData, String[] dataLake) {
        Map<Double, String[]> map = new HashMap<>(2);
        //将输入数据写入
        double[] X = new double[inputData.length];
        for (int i = 0; i < inputData.length; i++) {
            X[i] = Double.parseDouble(inputData[i]);
        }
        double dtw;
        //将比较数据数据湖写入
        double[] Y = new double[dataLake.length];
        for (int j = 0; j < dataLake.length; j++) {
            Y[j] = Double.parseDouble(dataLake[j]);
        }
        //计算dtw矩阵
        dtw = getDtwDist(X,Y);
        map.put(dtw, dataLake);
        return map;
    }

    /**
     * 距离矩阵
     */
    private static double computeDistance(double x, double y) {
        return Math.sqrt((x - y) * (x - y));
    }

    /**
     * 初始化距离矩阵
     */
    private static double[][] initDistance(double[] X, double[] Y) {
        double[][] distance = new double[X.length][Y.length];
        for (int i = 0; i < X.length; i++) {
            for (int j = 0; j < Y.length; j++) {
                distance[i][j] = computeDistance(X[i], Y[j]);
            }
        }
        return distance;
    }

    /**
     * 计算DTW矩阵
     */
    private static double[][] computeDtw(double[] X, double[] Y) {
        //初始化dtw数组
        double[][] dtw = new double[X.length][Y.length];
        double[][] distance = initDistance(X, Y);
        //根据distance数组来初始化dtw数组
        dtw[0][0] = 0;
        for (int i = 0; i < X.length; i++) {
            for (int j = 0; j < Y.length; j++) {
                //这里要对i,j进行判定，其实就是加入边界值的考虑
                if (i > 0 && j > 0) {
                    dtw[i][j] = minDist(
                            dtw[i][j - 1] + distance[i][j],
                            dtw[i - 1][j] + distance[i][j],
                            dtw[i - 1][j - 1] + 2 * distance[i][j]
                    );
                } else if (i == 0 && j > 0) {
                    dtw[i][j] = dtw[i][j - 1] + distance[i][j];
                } else if (i > 0 && j == 0) {
                    dtw[i][j] = dtw[i - 1][j] + distance[i][j];
                } else {
                    dtw[i][j] = 0;
                }
            }
        }
        return dtw;
    }

    /**
     * 计算DTW矩阵距离
     */
    public static double getDtwDist(double[] X, double[] Y) {
        double[][] dtw = computeDtw(X, Y);
        return dtw[X.length - 1][Y.length - 1];
    }

    /**
     * 计算DTW矩阵路线
     * @param dist1 右下
     * @param dist2 左下
     * @param dist3 斜下
     */
    private static double minDist(double dist1, double dist2, double dist3) {
        return Math.min(Math.min(dist1, dist2), dist3);
    }
}
