package com.team.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class CSVFile {
    private final String fileName;
    private MappedByteBuffer mappedByteBuffer;
    private Integer mappedByteBufferLen;

    private Integer colNums;
    private Integer rowNums;
    private Integer next;
    private Boolean readDone;

    private ArrayList<Integer> rowStart;

    public CSVFile(File file) {
        this.colNums = 0;
        this.rowNums = 0;
        this.next = 0;
        this.readDone = false;
        this.fileName = file.getName();
        this.rowStart = new ArrayList<>(1024);
        Long len = file.length();
        this.mappedByteBufferLen = len.intValue();

        try {
            this.mappedByteBuffer = new RandomAccessFile(file, "r")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, len);

            int idx = 0;
            while(mappedByteBuffer.get(idx) != '\n'){
                if(mappedByteBuffer.get(idx) == '|'){
                    this.colNums++;
                }
                idx++;
            }
            this.colNums++;
        } catch (IOException e) {
            System.out.println(e.toString());
        }

        if(this.mappedByteBuffer == null){
            System.out.println(fileName + " : mappedByteBuffer is null");
        }

    }

    public String get(Integer rowIdx, Integer colIdx){
        if(colIdx >= colNums){
            System.out.println("CSVFile get方法， 列越界");
            System.exit(1);
        }
        if(readDone && rowIdx >= rowNums){
            System.out.println("CSVFile get方法， 行越界");
            System.exit(1);
        }

        Integer index = rowStart.get(rowIdx);
        StringBuilder str = new StringBuilder();
        Integer colIndex = 0;
        while(colIndex < colIdx){
            while(mappedByteBuffer.get(index++) != '|') {}
            colIndex++;
        }
        char cur = (char)mappedByteBuffer.get(index);
        while(cur != '|' && cur != '\r' && cur != '\n'){
            str.append(cur);
            cur = (char)mappedByteBuffer.get(++index);
        }
        return str.toString();
    }

    public Boolean checkRowIndex(Integer rowIdx) {
        if(this.readDone && rowIdx >= rowNums) return false;
        if(rowIdx < this.rowNums) return true;
        Integer rowIndex = this.rowNums;
        while(rowIndex <= rowIdx){
            rowStart.add(this.next);
            while(mappedByteBuffer.get(this.next++) != '\n') {}
            rowIndex++;
            if(this.next >= mappedByteBufferLen){
                this.readDone = true;
                break;
            }
        }
        this.rowNums = rowIndex;

        return rowIdx < rowNums;
    }

//    public static void main(String[] args) throws IOException {
//        String fileName = "C:\\Users\\yangjx\\Desktop\\java\\ccf-contest\\database\\table2\\dt=aBZlVARrOg.cc\\demo0.txt";
//        File file = new File(fileName);
//        CSVFile csvFile = new CSVFile(file);
//        int rowIndex = 999;
//        if(csvFile.checkRowIndex(rowIndex)){
//            System.out.println(csvFile.get(rowIndex, 2));
//        }
//        rowIndex = 0;
//        if(csvFile.checkRowIndex(rowIndex)){
//            System.out.println(csvFile.get(rowIndex, 2));
//        }
//    }

}
