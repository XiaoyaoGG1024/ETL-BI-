/**
 * @program: BigData
 * @description: 二分查找
 * 给定一个 n 个元素有序的（升序）整型数组 nums 和一个目标值 target ，
 * 写一个函数搜索 nums 中的 target，如果目标值存在返回下标，否则返回 -1。
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/3
 **/
public class Binarysearch {
    public static int bsearchWithoutRecursion(int arr[], int key) {
        int low = 0;
        int high = arr.length - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (arr[mid] > key)
                high = mid - 1;
            else if (arr[mid] < key)
                low = mid + 1;
            else
                return mid;
        }
        return -1;
    }
    public static void main(String[] args) {
        int arr[] = {1,3,5,6,8,9,11,14,23};
        int num = bsearchWithoutRecursion(arr, 9);
        System.out.println(num);
    }
}
