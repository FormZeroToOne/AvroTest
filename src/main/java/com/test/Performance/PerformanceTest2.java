package com.test.Performance;

import com.test.avro.MyUser;
import com.test.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

/**
 * 反序列化测试时间,空间
 *
 * javaserial 和 writable , avro protobuf
 *
 * javaDeSerial : 55103
 * writableDeSerial : 39521
 * avroDeSerial : 1332
 * protoDeSerial : 8870
 *
 */
public class PerformanceTest2 {

    private static int max = 1000000;
    public static void main(String[] args) throws Exception {

        javaSerial();
        writableSerial();
        avroSerial();
        protoSerial();

    }


    /**
     * java序列化(内存)
     * @throws Exception
     */
    private  static  void  javaSerial() throws Exception{
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("E:/ADESKDOP/avro/users.java");
        //ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectInputStream oos = new ObjectInputStream(fis);
        MyUser user = null;
        for (int i = 0;i<max;i++){
             user = (MyUser) oos.readObject();
        }

        oos.close();
        System.out.println("javaDeSerial : "+(System.currentTimeMillis()-start));

    }


    /**
     * hadoop 中的
     * writableSerial 序列化
     */


    private  static void  writableSerial() throws Exception{
        long start = System.currentTimeMillis();
        //  ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream fis = new FileInputStream("E:/ADESKDOP/avro/users.writable");
        DataInputStream dos = new DataInputStream(fis);
        MyUser user = new MyUser();
        for (int i = 0;i<max;i++){
            user.readFields(dos);
        }

        dos.close();
        System.out.println("writableDeSerial : "+(System.currentTimeMillis()-start));

    }

    /**
     * 使用avro序列化
     */
    private static void avroSerial() throws Exception{
        long start = System.currentTimeMillis();
        File fis = new File("E:/ADESKDOP/avro/user.avro");
        DatumReader<User> reader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(fis,reader);
        User user1 = new User();
        while (dataFileReader.hasNext()){
            user1= dataFileReader.next();
        }




        dataFileReader.close();
        System.out.println("avroDeSerial : "+(System.currentTimeMillis()-start));
    }

    /**
     * 使用avro序列化
     */
    private static void protoSerial() throws Exception{
        long start = System.currentTimeMillis();
        FileInputStream fis = new FileInputStream("E:/ADESKDOP/avro/users.data");

        for (int i = 0 ; i< max;i++){
            com.test.proto.User.userProto u = com.test.proto.User.userProto.parseDelimitedFrom(fis);

        }
        System.out.println("protoDeSerial : "+(System.currentTimeMillis()-start));
    }



}
