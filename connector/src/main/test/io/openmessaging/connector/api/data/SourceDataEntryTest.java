package io.openmessaging.connector.api.data;


import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SourceDataEntryTest {

    /**
     * simple test for kv data
     * SET myset asldfjsaldjglas PX 360000
     */
    @Test
    public void testKV(){
        String command = "SET";
        String key = "myset";
        String value = "asldfjsaldjglas";
        long px = 360000;


        DataEntryBuilder builder =
            DataEntryBuilder.newDataEntryBuilder()
                        .keyMeta(Meta.STRING_META)
                        .keyData(key)
                        .valueMeta(Meta.STRING_META)
                        .valueData(value)
                        .header("REDIS_COMMAND", command)
                        .header("PX", px);

        SourceDataEntry sourceDataEntry = builder.buildSourceDataEntry(
            ByteBuffer.wrap("partition1".getBytes()),
            ByteBuffer.wrap("1098".getBytes())
        );

        assertNotNull(sourceDataEntry);
        assertEquals(sourceDataEntry.getKey().convertToString(), "myset");
        assertEquals(sourceDataEntry.getValue().convertToString(), "asldfjsaldjglas");
        assertEquals(sourceDataEntry.getHeaders().findHeader("REDIS_COMMAND").data().convertToString(), "SET");
        assertTrue(sourceDataEntry.getHeaders().toMap().get("PX").data().convertToLong() == 360000);
        //System.out.println(sourceDataEntry);
    }

    /**
     * simple test for table data
     */
    @Test
    public void testTable(){
        // construct table meta
        Meta tableMeta = MetaBuilder.struct()
                .field("id", Meta.INT64_META)
                .field("name", Meta.STRING_META)
                .field("age", Meta.INT16_META)
                .field("score", Meta.INT32_META)
                .field("isNB", Meta.BOOLEAN_META)
                .build();

        // construct sourceDataEntry
        DataEntryBuilder builder =
            DataEntryBuilder.newDataEntryBuilder(tableMeta)
                .valueData("id", 1L)
                .valueData("name", "小红")
                .valueData("age", (short)17)
                .valueData("score", 99)
                .valueData("isNB", true);

        SourceDataEntry sourceDataEntry = builder.buildSourceDataEntry(
            ByteBuffer.wrap("partition1".getBytes()),
            ByteBuffer.wrap("1098".getBytes())
        );

        assertNotNull(sourceDataEntry);
        assertNotNull(sourceDataEntry);
        assertTrue(sourceDataEntry.getValue().convertToStruct().getInt64("id") == 1L);
        assertEquals(sourceDataEntry.getValue().convertToStruct().getString("name"), "小红");
        assertTrue(sourceDataEntry.getValue().convertToStruct().getInt16("age") == (short)17);
        assertTrue(sourceDataEntry.getValue().convertToStruct().getInt32("score") == 99);
        assertTrue(sourceDataEntry.getValue().convertToStruct().getBoolean("isNB"));
        //System.out.println(sourceDataEntry);
    }

    /**
     * test data for type of struct
     *
     * @return
     */
    @Test
    public void testStruct(){
        // 1. construct meta

        DataEntryBuilder structDataEntry = new DataEntryBuilder(
            Meta.STRING_META,
            MetaBuilder.struct()
                .name("myStruct")
                .field("field_string", Meta.STRING_META)
                .field("field_int32", Meta.INT32_META)
                .parameter("parameter", "sadfsadf")
                .build()
        );

        // 2. construct data

        structDataEntry
            .queue("last_queue")
            .queueId(1)
            .timestamp(System.currentTimeMillis())
            .entryType(EntryType.UPDATE)
            .keyData("schema_data_lalala")
            .valueData("field_string", "nihao")
            .valueData("field_int32", 321)
            .header("int_header", 1)
            ;

        // 3. construct dataEntry

        SourceDataEntry sourceDataEntry = structDataEntry.buildSourceDataEntry(null, null);

        assertNotNull(sourceDataEntry);
        //System.out.println(sourceDataEntry);
    }



}
