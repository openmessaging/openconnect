/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.connector.api.errors;

import java.text.MessageFormat;

public class ConnectException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    private String code;

    public ConnectException(String msg) {
        super(MessageFormat.format(DefaultConnectError.InternalError.getMessage(), msg));
        this.code = DefaultConnectError.InternalError.getCode();
    }

    public ConnectException(String msg, Throwable throwable) {
        super(MessageFormat.format(DefaultConnectError.InternalError.getMessage(), msg), throwable);
        this.code = DefaultConnectError.InternalError.getCode();
    }

    public ConnectException(Throwable throwable) {
        super(throwable);
        this.code = DefaultConnectError.InternalError.getCode();
    }

    public ConnectException(ConnectErrors connectErrors, Throwable throwable, Object... args) {
        super(MessageFormat.format(connectErrors.getMessage(), args), throwable);
        this.code = connectErrors.getCode();
    }

    public ConnectException(ConnectErrors connectErrors, Object... args) {
        super(MessageFormat.format(connectErrors.getMessage(), args));
        this.code = connectErrors.getCode();
    }
}
