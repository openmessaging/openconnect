package io.openmessaging.connector.api.data;

import java.math.BigDecimal;
import java.util.Map;

import io.openmessaging.connector.api.header.DataHeader;
import io.openmessaging.connector.api.header.DataHeaders;
import io.openmessaging.connector.api.header.Header;
import io.openmessaging.connector.api.header.Headers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HeadersTest {


    @Test
    public void commonTest(){
        Meta strMeta = MetaBuilder.string().name("myStrMeta").build();
        Header header = new DataHeader("header1", strMeta, "headerValue1");
        //System.out.println("construct header: " + header);
        assertNotNull(header);

        Headers headers = new DataHeaders();
        headers.add(header)
            .add("header1", strMeta, "headerValue2")
            .addBoolean("header2", true)
            .addDecimal("header3 ", BigDecimal.ONE)
        ;
        //System.out.println("construct headers: " + headers);
        assertNotNull(headers);

        Header findHeader1 = headers.findHeader("header1");
        //System.out.println("except header named header1, real is: " + findHeader1);
        assertEquals(findHeader1.key(), "header1");
        findHeader1.rename("header2");

        Header findHeader2 = headers.findHeader("header2");
        //System.out.println("except header valued TRUE, real is: " + findHeader2);
        assertEquals(findHeader2.key(), "header2");

        Map<String, Header> headerMap = headers.toMap();
        //System.out.println("show header Map: " + headerMap);
        assertNotNull(headerMap);
        headerMap.put("header3", header);
    }

}
