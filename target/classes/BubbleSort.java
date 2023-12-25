import java.util.Arrays;

/**
 * @program: BigData
 * @description: 冒泡排序
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/3
 **/
public class BubbleSort {
    public static void main(String[] args) {
        int[] arr = new int[] { 2, 8, 7, 9, 4, 1, 5, 0 };
        bubbleSort(arr);
    }
    public static void bubbleSort(int[] arr) {
//控制多少轮
        for (int i = 1; i < arr.length; i++) {
//控制每一轮的次数
            for (int j = 0; j < arr.length - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp;
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        System.out.println(Arrays.toString(arr));
    }

}
