package com.test;

import com.test.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;

import java.io.File;

/**
 * 不生成源代码，直接通过schema进行序列化
 */
public class sourceCode {
    public static void main(String[] args) throws Exception{
        //加载schema文件
        Schema schema = new Schema.Parser().parse(new File("E://project//myavro//src//main//avro//user.avsc"));

        //常规Record文件
        //通过解析avro文件，进行创建对象
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name","tom");
        user1.put("favorite_number",350);
        user1.put("favorite_color","blue");


        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name","jack");
        user2.put("favorite_number",250);
        user2.put("favorite_color","green");


        //序列化文件
        File file =  new File("E://project//myavro//src//main//avro//user2.avro");
        DatumWriter<GenericRecord>  datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema,file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
        System.out.println("over!!!!!!!!!!!!!");





    }

    /**
     * 反序列化
     */
    @Test
    public  void DeSourceCode() throws Exception{
        Schema schema = new Schema.Parser().parse(new File("E://project//myavro//src//main//avro//user.avsc"));
        File file =  new File("E://project//myavro//src//main//avro//user2.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()){
            user = dataFileReader.next(user);
            System.out.println(user.get("favorite_color"));
        }


    }

}
