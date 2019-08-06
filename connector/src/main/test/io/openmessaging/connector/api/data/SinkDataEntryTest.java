package io.openmessaging.connector.api.data;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SinkDataEntryTest {

    /**
     * simple test for kv data
     * SET myset asldfjsaldjglas PX 360000
     */
    @Test
    public void testKV() {
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

        SinkDataEntry sinkDataEntry = builder.buildSinkDataEntry(
            1500L
        );

        assertNotNull(sinkDataEntry);
        assertEquals(sinkDataEntry.getKey().convertToString(), "myset");
        assertEquals(sinkDataEntry.getValue().convertToString(), "asldfjsaldjglas");
        assertEquals(sinkDataEntry.getHeaders().findHeader("REDIS_COMMAND").data().convertToString(), "SET");
        assertTrue(sinkDataEntry.getHeaders().toMap().get("PX").data().convertToLong() == 360000);
    }

    /**
     * simple test for table data
     */
    @Test
    public void testTable() {
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

        SinkDataEntry sinkDataEntry = builder.buildSinkDataEntry(1500L);

        //System.out.println(sinkDataEntry);
        assertNotNull(sinkDataEntry);
        assertTrue(sinkDataEntry.getValue().convertToStruct().getInt64("id") == 1L);
        assertEquals(sinkDataEntry.getValue().convertToStruct().getString("name"), "小红");
        assertTrue(sinkDataEntry.getValue().convertToStruct().getInt16("age") == (short)17);
        assertTrue(sinkDataEntry.getValue().convertToStruct().getInt32("score") == 99);
        assertTrue(sinkDataEntry.getValue().convertToStruct().getBoolean("isNB"));
    }

    /**
     * test data for type of struct
     *
     * @return
     */
    @Test
    public void testStruct() {
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

        SinkDataEntry sinkDataEntry = structDataEntry.buildSinkDataEntry(1500L);

        //System.out.println(sinkDataEntry);
        assertNotNull(sinkDataEntry);
    }

}
