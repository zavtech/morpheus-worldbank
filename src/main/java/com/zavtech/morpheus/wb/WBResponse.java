/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 *
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
package com.zavtech.morpheus.wb;

/**
 * A class that represents a response message from an World Bank API service.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBResponse<T> implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private T body;
    private WBHeader header;

    /**
     * Constructor
     * @param header    the header for this response
     * @param body      the body for this response
     */
    public WBResponse(WBHeader header, T body) {
        this.header = header;
        this.body = body;
}

    /**
     * Returns the header for this response
     * @return      the header for response
     */
    public WBHeader getHeader() {
        return header;
    }

    /**
     * Returns the body for this response
     * @return      the body for this response
     */
    public T getBody() {
        return body;
    }

    /**
     * Returns how many pages required to load all data given a records per page assumption
     * @param recordsPerPage    the records per page
     * @return                  the number of requests
     */
    public int getRequestCount(int recordsPerPage) {
        final double totalRecords = getHeader().getTotalRecords();
        return Math.max(1, (int)Math.ceil(totalRecords / (double)recordsPerPage));
    }

}
