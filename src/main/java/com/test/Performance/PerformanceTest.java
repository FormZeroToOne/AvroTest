package com.test.Performance;

import com.test.avro.MyUser;
import com.test.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

/**
 * 性能评测
 *序列化到磁盘或内存中，都与之比较
 * java 和 writable, avro 执行时间
 *
 * javaSerial : 97642
 * writableSerial : 143279
 * avroSerial : 2764
 * protoSerial : 47230
 */
public class PerformanceTest {

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
        FileOutputStream fos = new FileOutputStream("E:/ADESKDOP/avro/users.java");
        //ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        for (int i = 0;i < max; i++){
            MyUser user = new MyUser();
            user.setName("tom"+i);
            user.setFavorite_number(i);
            user.setFavorite_color("blue"+i);
            oos.writeObject(user);
        }

        oos.close();
        //System.out.println("javaSerial : "+(System.currentTimeMillis()-start)+" : "+baos.toByteArray().length);
        System.out.println("javaSerial : "+(System.currentTimeMillis()-start));

    }


    /**
     * hadoop 中的
     * writableSerial 序列化
     */


    private  static void  writableSerial() throws Exception{
        long start = System.currentTimeMillis();
      //  ByteArrayOutputStream baos = new ByteArrayOutputStream();
       FileOutputStream fos = new FileOutputStream("E:/ADESKDOP/avro/users.writable");
        DataOutputStream dos = new DataOutputStream(fos);
        MyUser user = null;
        for (int i = 0;i < max; i++){
         user = new MyUser("tom"+i,i,"blue"+i);
         user.write(dos);
        }

        dos.close();
        System.out.println("writableSerial : "+(System.currentTimeMillis()-start));

    }

    /**
     * 使用avro序列化
     */
    private static void avroSerial() throws Exception{
        long start = System.currentTimeMillis();
       // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(writer);
        User user1 = new User();
        dataFileWriter.create(user1.getSchema(),new FileOutputStream("E:/ADESKDOP/avro/user.avro"));
        //dataFileWriter.create(user1.getSchema(),baos);


        for (int i = 0;i < max; i++){
            User users = new User();
            users.setName("tom"+i);
            users.setFavoriteNumber(i);
            users.setFavoriteColor("blue"+i);
            dataFileWriter.append(users);
        }


        dataFileWriter.close();
        System.out.println("avroSerial : "+(System.currentTimeMillis()-start));
    }

    /**
     * 使用protobuf序列化
     */

    private static void protoSerial() throws Exception{
        FileOutputStream fos = new FileOutputStream("E:/ADESKDOP/avro/users.data");
        long start = System.currentTimeMillis();
        for (int i = 0;i <max;i++) {
            com.test.proto.User.userProto upo =com.test.proto.User.userProto.newBuilder().setName("tom")
                                                      .setFavoriteNumber(i)
                                                      .setFavoriteColor("red"+i).build();


            upo.writeDelimitedTo(fos);
        }
        fos.close();
        System.out.println("protoSerial : "+(System.currentTimeMillis()-start));
    }








}

