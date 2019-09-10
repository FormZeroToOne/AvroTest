package com.test.Serialize;

import com.test.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

public class DeSerializeTest {
    public static void main(String[] args)  throws Exception{

        File file = new File("E:/project/myavro/src/main/avro/user.avro");
        DatumReader<User>  userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file,userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()){
            user = dataFileReader.next(user);
            System.out.println(user.getName());
        }



    }

}
