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

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * A class that represents a header on a World Bank response message
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBHeader implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private int pageNumber;
    private int pageCount;
    private int recordCount;
    private int totalRecords;

    /**
     * Constructor
     * @param pageNumber    the page number for response
     * @param pageCount     the total page count
     * @param recordCount   the number of records for this page
     * @param totalRecords  the total number of records
     */
    public WBHeader(int pageNumber, int pageCount, int recordCount, int totalRecords) {
        this.pageNumber = pageNumber;
        this.pageCount = pageCount;
        this.recordCount = recordCount;
        this.totalRecords = totalRecords;
    }

    /**
     * Returns the page number for this response
     * @return      the page number for response
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * Returns the total number of pages
     * @return  the total number of pages
     */
    public int getPageCount() {
        return pageCount;
    }

    /**
     * Returns the number of data points per page
     * @return      the number of data points per page
     */
    public int getRecordCount() {
        return recordCount;
    }

    /**
     * Returns the number of records for this page
     * @return      the number of records for this page
     */
    public int getTotalRecords() {
        return totalRecords;
    }


    @Override
    public String toString() {
        return "WBHeader{" +
                "pageNumber=" + getPageNumber() +
                ", pageCount=" + getPageCount() +
                ", recordCount=" + getRecordCount() +
                ", totalRecords=" + getTotalRecords() +
                '}';
    }

    /**
     * A deserializer for WBIndicator objects
     */
    public static class Deserializer implements JsonDeserializer<WBHeader> {
        @Override
        public WBHeader deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            try {
                final JsonObject object = element.getAsJsonObject();
                final int pageNumber = object.get("page").isJsonNull() ? 0 : object.get("page").getAsInt();
                final int pageCount = object.get("pages").isJsonNull() ? 0 : object.get("pages").getAsInt();
                final int recordCount = object.get("per_page").isJsonNull() ? 0 : object.get("per_page").getAsInt();
                final int totalRecords = object.get("total").isJsonNull() ? 0 : object.get("total").getAsInt();
                return new WBHeader(pageNumber, pageCount, recordCount, totalRecords);
            } catch (Exception ex) {
                throw new WBException("Failed to deserialize WBIndicator", ex);
            }
        }
    }
}
