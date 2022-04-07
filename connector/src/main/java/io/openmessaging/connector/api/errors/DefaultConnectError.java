/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.openmessaging.connector.api.errors;

public enum DefaultConnectError implements ConnectErrors {
    /**
     * Default internal error.
     */
    InternalError("InternalError", "{0}"),
    /**
     * Source connector commit error.
     */
    SOURCE_CONNECTOR_COMMIT_ERROR("SourceConnectorPollError", "Source Connector commit connector records error."),
    /**
     * Source connector poll error.
     */
    SOURCE_CONNECTOR_POLL_ERROR("SourceConnectorPollError", "Source Connector poll connector records error."),
    /**
     * Sink connector put error.
     */
    SINK_CONNECTOR_PUT_ERROR("SinkConnectorPutError", "Sink Connector put connector records error."),

    /**
     * Sts certificate fetch error.
     */
    STS_CERTIFICATE_FETCH_ERROR("StsCertificateFetchError", "Sts certificate fetch error."),
    /**
     * Not a json element.
     */
    NOT_JSON_ELEMENT("NotJsonElement", "The connect=[{0}] can not parse to json element."),
    /**
     * Invalid data.
     */
    INVALID_DATA("InvalidData", "The data is invalid ,which is:{0}");

    private String code;

    private String message;

    DefaultConnectError(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
