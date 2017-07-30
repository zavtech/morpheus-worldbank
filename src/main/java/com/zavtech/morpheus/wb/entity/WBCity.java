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
 * A class that represents a World Bank defined City with geo-location
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCity implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private String longitude;
    private String latitude;

    /**
     * Constructor
     * @param name          the city name
     * @param longitude     the longitude coordinate
     * @param latitude      the latitude coordinate
     */
    public WBCity(String name, String longitude, String latitude) {
        this.name = name;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    /**
     * Returns the city name
     * @return      the city name
     */
    public String getName() {
        return name;
    }

    /**
     * The longitude coordinates
     * @return  the longitude coordinates
     */
    public String getLongitude() {
        return longitude;
    }

    /**
     * The latitude coordinates
     * @return  the latitude coordinates
     */
    public String getLatitude() {
        return latitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBCity)) return false;
        final WBCity wbCity = (WBCity) o;
        return getName().equals(wbCity.getName());
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }


    @Override
    public String toString() {
        return "WBCity{" + "name='" + name + '\'' + ", longitude='" + longitude + '\'' + ", latitude='" + latitude + '\'' + '}';
    }


    /**
     * A deserializer for WBRegion objects
     */
    public static class Deserializer implements JsonDeserializer<WBCity> {
        @Override
        public WBCity deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            final JsonObject object = element.getAsJsonObject();
            final String name = object.get("capitalCity").getAsString();
            final String longitude = object.get("longitude").getAsString();
            final String latitude = object.get("latitude").getAsString();
            if (name != null && name.trim().length() > 0) {
                return new WBCity(name, longitude, latitude);
            } else {
                return null;
            }
        }
    }

}
