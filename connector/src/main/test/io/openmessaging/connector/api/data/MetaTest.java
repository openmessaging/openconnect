package io.openmessaging.connector.api.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MetaTest {

    @Test
    public void staticMethod(){
        Type type = Meta.getMetaType(String.class);
        assertEquals(type, Type.STRING);
        type = Meta.getMetaType(Struct.class);
        assertEquals(type, Type.STRUCT);
        type = Meta.getMetaType(Map.class);
        assertEquals(type, Type.MAP);
        type = Meta.getMetaType(ArrayList.class);
        assertEquals(type, Type.ARRAY);



        Meta.validateValue(Meta.BYTES_META, "kfc".getBytes());
        Exception error = null;
        try{
            // Throw exception for the value of STRING do not match the meta of BYTES .
            Meta.validateValue(Meta.BYTES_META, "kfc");
        }catch (Exception e){
            error = e;
        }
        assertNotNull(error);

        Meta.validateValue("name", Meta.STRING_META, "fasd");

        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        Object obj = list;
        Meta.validateValue(MetaBuilder.array(Meta.STRING_META).build(), obj);

        error = null;
        try{
            // Throw exception for the value of ARRAY do not match the meta of BYTES .
            Meta.validateValue(MetaBuilder.array(Meta.BYTES_META).build(), obj);
        }catch (Exception e){
            error = e;
        }
        assertNotNull(error);
    }

    @Test
    public void int8(){
        System.out.println("================INT8================");
        Meta meta1 = MetaBuilder.int8()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.int8()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.int8()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.int8()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.INT8, "abc",  1,"dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.INT8_META);
    }

    @Test
    public  void int16(){
        System.out.println("================INT16================");
        Meta meta1 = MetaBuilder.int16()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.int16()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.int16()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.int16()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.INT16, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.INT16_META);
    }


    @Test
    public  void int32(){
        System.out.println("================INT32================");
        Meta meta1 = MetaBuilder.int32()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.int32()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.int32()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.int32()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.INT32, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.INT32_META);
    }



    @Test
    public  void int64(){
        System.out.println("================INT64================");
        Meta meta1 = MetaBuilder.int64()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.int64()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.int64()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.int64()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.INT64, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.INT64_META);
    }




    @Test
    public  void float32(){
        System.out.println("================FLOAT32================");
        Meta meta1 = MetaBuilder.float32()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.float32()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.float32()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.float32()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.FLOAT32, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.FLOAT32_META);
    }


    @Test
    public  void float64(){
        System.out.println("================FLOAT64================");
        Meta meta1 = MetaBuilder.float64()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.float64()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.float64()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.float64()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.FLOAT64, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.FLOAT64_META);
    }


    @Test
    public void bool(){
        System.out.println("================BOOLEAN================");
        Meta meta1 = MetaBuilder.bool()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.bool()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.bool()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.bool()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.BOOLEAN, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.BOOLEAN_META);
    }


    @Test
    public void str(){
        System.out.println("================STRING================");
        Meta meta1 = MetaBuilder.string()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.string()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.string()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.string()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.STRING, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.STRING_META);
    }


    @Test
    public void bytes(){
        System.out.println("================BYTES================");
        Meta meta1 = MetaBuilder.bytes()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.bytes()
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.bytes()
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.bytes()
            .build();
        System.out.println(meta4);

        Meta meta = new MetaBase(Type.BYTES, "abc",  1, "dataSource", null);
        System.out.println(meta);

        System.out.println(Meta.BYTES_META);
    }


    @Test
    public void array(){
        System.out.println("================ARRAY================");
        Meta meta1 = MetaBuilder.array(Meta.STRING_META)
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.array(Meta.STRING_META)
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.array(Meta.STRING_META)
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.array(Meta.STRING_META)
            .build();
        System.out.println(meta4);

        Meta meta = new MetaArray(Type.ARRAY, "abc",  1, "dataSource",
            null, Meta.STRING_META);
        System.out.println(meta);

        try{
            Meta metaError = new MetaMap(Type.MAP, "abc",  1, "dataSource",
                null, Meta.STRING_META, Meta.BYTES_META);
        }catch (Exception e){
            System.out.println("error for new MataMap but Type is not ARRAY.");
        }
    }


    @Test
    public void map(){
        System.out.println("================MAP================");
        Meta meta1 = MetaBuilder.map(Meta.STRING_META, Meta.BYTES_META)
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.map(Meta.STRING_META, Meta.BYTES_META)
            .name("build2")
            .dataSource("datasource1")
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.map(Meta.STRING_META, Meta.BYTES_META)
            .dataSource("datasource1")
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.map(Meta.STRING_META, Meta.BYTES_META)
            .build();
        System.out.println(meta4);

        Meta meta = new MetaMap(Type.MAP, "abc",  1, "dataSource",
            null, Meta.STRING_META, Meta.BYTES_META);
        System.out.println(meta);

        try{
            Meta metaError = new MetaMap(Type.ARRAY, "abc",  1, "dataSource",
                null, Meta.STRING_META, Meta.BYTES_META);
        }catch (Exception e){
            System.out.println("error for new MataMap but Type is not MAP.");
        }

    }


    @Test
    public void struct(){
        System.out.println("================STRUCT================");
        Meta meta1 = MetaBuilder.struct()
            .name("build1")
            .version(100)
            .dataSource("datasource1")
            .parameter("p1","v1")
            .parameter("p2", "v2")
            .field("f1", Meta.STRING_META)
            .field("f2", Meta.BOOLEAN_META)
            .field("f3", Meta.FLOAT32_META)
            .build();
        System.out.println(meta1);

        Meta meta2 = MetaBuilder.struct()
            .name("build2")
            .dataSource("datasource1")
            .field("f1", Meta.STRING_META)
            .field("f2", Meta.BOOLEAN_META)
            .field("f3", Meta.FLOAT32_META)
            .build();
        System.out.println(meta2);

        Meta meta3 = MetaBuilder.struct()
            .dataSource("datasource1")
            .field("f1", Meta.STRING_META)
            .field("f2", Meta.BOOLEAN_META)
            .field("f3", Meta.FLOAT32_META)
            .build();
        System.out.println(meta3);

        Meta meta4 = MetaBuilder.struct()
            .field("f1", Meta.STRING_META)
            .field("f2", Meta.BOOLEAN_META)
            .field("f3", Meta.FLOAT32_META)
            .build();
        System.out.println(meta4);

        Meta meta5 = MetaBuilder.struct()
            .build();
        System.out.println(meta5);

        int size = 10;
        List<Field> fields = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Field field = new Field(i, "f"+i, Meta.STRING_META);
            fields.add(field);
        }
        Meta meta = new MetaStruct(Type.STRUCT, "abc",  1, "dataSource",
            null, fields);
        System.out.println(meta);

        try{
            Meta metaError = new MetaStruct(Type.MAP, "abc",  1, "dataSource",
                null, fields);
        }catch (Exception e){
            System.out.println("error for new MetaStruct but Type is not STRUCT.");
        }

    }

}
