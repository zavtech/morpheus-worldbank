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
package com.zavtech.morpheus.wb.entity;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * A class that defines a World Bank income level definition
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBIncomeLevel implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String value;

    /**
     * Constructor
     * @param id        the id
     * @param value     the value
     */
    public WBIncomeLevel(String id, String value) {
        this.id = id;
        this.value = value;
    }

    /**
     * Returns the income level identifier
     * @return      the income level id
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the income level long name
     * @return      the long name
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBIncomeLevel)) return false;
        final WBIncomeLevel that = (WBIncomeLevel) o;
        return getId().equals(that.getId());

    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }


    @Override
    public String toString() {
        return "WBIncomeLevel{" + "id='" + id + '\'' + ", value='" + value + '\'' + '}';
    }


    /**
     * A deserializer for WBIncomeLevel objects
     */
    public static class Deserializer implements JsonDeserializer<WBIncomeLevel> {
        @Override
        public WBIncomeLevel deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            final JsonObject object = element.getAsJsonObject();
            final String id = object.get("id").getAsString();
            final String value = object.get("value").getAsString();
            if (id != null && id.trim().length() > 0) {
                return new WBIncomeLevel(id, value);
            } else {
                return null;
            }
        }
    }

}
