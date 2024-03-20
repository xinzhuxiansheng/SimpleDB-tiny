package yzhou;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 将 txt 转成 二进制二年
 */
public class TextToBinaryFile {
    public static void main(String[] args) {
        // 输入文件路径
        String inputFilePath = "/Users/a/Code/Java/simpledb-tiny/files/some_data_file.txt";
        // 输出二进制文件路径
        String outputFilePath = "/Users/a/Code/Java/simpledb-tiny/files/some_data_file.dat";

        try (FileInputStream fis = new FileInputStream(inputFilePath);
             FileOutputStream fos = new FileOutputStream(outputFilePath)) {

            // 读取数据并写入到二进制文件
            int byteData;
            while ((byteData = fis.read()) != -1) {
                fos.write(byteData);
            }
            System.out.println("File conversion completed.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
