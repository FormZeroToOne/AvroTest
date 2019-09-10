package com.test.Serialize;

import com.test.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class SerializeTest {

    public static void main(String[] args)  throws Exception{
        //创建user的第一种方式
        User user = new User();
        user.setName("张三");
        user.setFavoriteColor("red");
        user.setFavoriteNumber(20);

       //第二种
        User user2 =User.newBuilder()
                    .setName("李四")
                    .setFavoriteColor("blue")
                    .setFavoriteNumber(19)
                    .build();

        //第三种
        User user3 = new User("王五",20,"green");

        //通过writer写入文件到磁盘
        DatumWriter<User>  userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User>  userDataFileWriter = new DataFileWriter<>(userDatumWriter);
        userDataFileWriter.create(user.getSchema(),new File("E:/project/myavro/src/main/avro/user.avro"));
        userDataFileWriter.append(user);
        userDataFileWriter.append(user2);
        userDataFileWriter.append(user3);
        userDataFileWriter.close();





    }
}
