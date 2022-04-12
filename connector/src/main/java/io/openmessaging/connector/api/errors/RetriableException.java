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

public class RetriableException extends ConnectException {

    private int putIndex;

    public int getPutIndex() {
        return putIndex;
    }

    public void setPutIndex(int putIndex) {
        this.putIndex = putIndex;
    }

    public RetriableException(String msg, int putIndex) {
        super(msg);
        this.putIndex = putIndex;
    }

    public RetriableException(String msg) {
        super(msg);
    }

    public RetriableException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

    public RetriableException(String msg, int putIndex, Throwable throwable) {
        super(msg, throwable);
        this.putIndex = putIndex;
    }

    public RetriableException(Throwable throwable) {
        super(throwable);
    }

    public RetriableException(ConnectErrors connectErrors, Throwable throwable, Object... args) {
        super(connectErrors, throwable, args);
    }

    public RetriableException(ConnectErrors connectErrors, Object... args) {
        super(connectErrors, args);
    }
}
