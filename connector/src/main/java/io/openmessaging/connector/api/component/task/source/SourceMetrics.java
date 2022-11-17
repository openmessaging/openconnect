package io.openmessaging.connector.api.component.task.source;
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

import java.util.Map;

public class SourceMetrics {
    /**
     * The delay time of source
     */
    private Long delayTime;
    /**
     * The diff offset of source
     */
    private Long diffOffset;

    /**
     * other metrics
     */
    private Map<String, Object> others;

    public Long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Long delayTime) {
        this.delayTime = delayTime;
    }

    public Long getDiffOffset() {
        return diffOffset;
    }

    public void setDiffOffset(Long diffOffset) {
        this.diffOffset = diffOffset;
    }

    public Map<String, Object> getOthers() {
        return others;
    }

    public void setOthers(Map<String, Object> others) {
        this.others = others;
    }
}
