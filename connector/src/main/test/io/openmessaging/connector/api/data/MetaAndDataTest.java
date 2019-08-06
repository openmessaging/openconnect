package io.openmessaging.connector.api.data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MetaAndDataTest {


    @Test
    public void structParserTest(){
        Meta structMeta = MetaBuilder.struct()
            .name("structMeta")
            .field("f1", Meta.STRING_META)
            .field("f2", Meta.INT32_META)
            .field("f3", MetaBuilder.array(Meta.STRING_META).build())
            .build();
        MetaAndData structMetaAndData = new MetaAndData(
            structMeta
        );

        structMetaAndData.putData("f1", "asdf");
        structMetaAndData.putData("f2", 32);
        structMetaAndData.putData("f3", new ArrayList<String>(){{
            add("a");
            add("b");
        }});

        String jsonStr = structMetaAndData.convertToString();
        assertNotNull(jsonStr);
        //System.out.println(jsonStr);

        // MAP MetaAndData
        MetaAndData newMetaAndData = MetaAndData.getMetaDataFromString(jsonStr);
        assertNotNull(newMetaAndData);
        //System.out.println(newMetaAndData);

        // MAP TO STRUCT
        Struct struct = newMetaAndData.convertToStruct();
        assertNotNull(struct);
        //System.out.println(struct);
    }

    @Test
    public void listParserTest(){
        String str = "[1, 2, 3, \"four\"]";
        MetaAndData result = MetaAndData.getMetaDataFromString(str);

        List<?> list = (List<?>) result.getData();
        //System.out.println(list);
        assertEquals(4, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals(2, ((Number) list.get(1)).intValue());
        assertEquals(3, ((Number) list.get(2)).intValue());
        assertEquals("four", list.get(3));
    }

    @Test
    public void commonTest(){
        MetaAndData metaAndData = new MetaAndData(Decimal.builder(10).build(), BigDecimal.ONE);
        assertNotNull(metaAndData);

        BigDecimal de = metaAndData.convertToDecimal(10);
        assertEquals(de, BigDecimal.ONE);
        assertNotEquals(de, 1);

        BigDecimal de2 = metaAndData.convertToDecimal(1);
        assertEquals(de2, BigDecimal.ONE);

        metaAndData.putData(BigDecimal.TEN);
        assertEquals(metaAndData.convertToDecimal(10), BigDecimal.TEN);

        assertEquals((BigDecimal)metaAndData.getData(), BigDecimal.TEN);

        assertNull(metaAndData.inferMeta());
    }

    @Test
    public void stringTest(){
        MetaAndData metaAndData = new MetaAndData(Meta.STRING_META, "message value 中文信息");
        assertNotNull(metaAndData);

        String msg = metaAndData.convertToString();
        assertEquals(msg, "message value 中文信息");

        Exception ex = null;
        try{
            metaAndData.convertToDouble();
        }catch (Exception e){
            ex = e;
        }
        assertNotNull(ex);
    }




}
