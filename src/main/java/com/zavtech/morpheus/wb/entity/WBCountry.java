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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;

/**
 * Represents a World Bank Country / Region definition
 *
 * http://api.worldbank.org/countries?format=json
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCountry implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    public enum Field {
        ID,
        ISO2,
        NAME,
        CAPITAL,
        LONGITUDE,
        LATITUDE,
        REGION_ID,
        REGION_NAME,
        LENDING_TYPE_ID,
        LENDING_TYPE_NAME,
        INCOME_LEVEL_ID,
        INCOME_LEVEL_NAME
    }

    private String id;
    private String iso2Code;
    private String name;
    private WBRegion region;
    private WBCity capitalCity;
    private WBLendingType lendingType;
    private WBIncomeLevel incomeLevel;

    /**
     * Constructor
     * @param id            the country identifier
     * @param iso2Code      the ISO 2 code
     * @param name          the country name
     * @param region        the region definition
     * @param capitalCity   the capital city
     * @param lendingType   the lending type
     * @param incomeLevel   the income level
     */
    public WBCountry(String id, String iso2Code, String name, WBRegion region, WBCity capitalCity, WBLendingType lendingType, WBIncomeLevel incomeLevel) {
        this.id = id;
        this.iso2Code = iso2Code;
        this.name = name;
        this.region = region;
        this.capitalCity = capitalCity;
        this.lendingType = lendingType;
        this.incomeLevel = incomeLevel;
    }


    /**
     * Returns a list of all World Bank defined countries
     * @return      the list of all World Bank countries
     */
    public static List<WBCountry> getCountries() {
        final WBLoader loader = new WBLoader();
        final String url = "http://api.worldbank.org/countries?format=json&per_page=1000";
        return loader.load(url, reader -> {
            try {
                final Gson gson = loader.builder().create();
                final List<WBCountry> results = new ArrayList<>(1000);
                while (reader.hasNext()) {
                    WBCountry country = gson.fromJson(reader, WBCountry.class);
                    if (country != null) {
                        results.add(country);
                    }
                }
                return results;
            } catch (Exception ex) {
                throw new WBException("Failed to extract country records for " + url, ex);
            }
        }).getBody();
    }


    /**
     * Returns the World Bank ID for this country
     * @return      the World Bank id for country
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the country name
     * @return  the country name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the ISO 2 code for country
     * @return      the ISO 2 code
     */
    public String getIso2Code() {
        return iso2Code;
    }

    /**
     * Returns true if this represents an aggregation of countries
     * @return      true if this represents an aggregate
     */
    public boolean isAggregate() {
        return region == null;
    }

    /**
     * Returns the optional region for this country
     * @return      the optional region
     */
    public Optional<WBRegion> getRegion() {
        return Optional.ofNullable(region);
    }

    /**
     * Returns the optional capital city for region
     * @return  the capital city, empty if is aggregate
     */
    public Optional<WBCity> getCapitalCity() {
        return Optional.ofNullable(capitalCity);
    }

    /**
     * Returns the lending type for this country
     * @return  the lending type, empty if aggregate
     */
    public Optional<WBLendingType> getLendingType() {
        return Optional.ofNullable(lendingType);
    }

    /**
     * Returns the optional income level for country
     * @return  the income level for country
     */
    public Optional<WBIncomeLevel> getIncomeLevel() {
        return Optional.ofNullable(incomeLevel);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBCountry)) return false;
        final WBCountry wbCountry = (WBCountry) o;
        return id.equals(wbCountry.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }


    @Override
    public String toString() {
        return "WBCountry{" + "id='" + id + '\'' + ", iso2Code='" + iso2Code + '\'' + ", name='" + name + '\'' + '}';
    }

    /**
     * A deserializer for WBCountry objects
     */
    public static class Deserializer implements JsonDeserializer<WBCountry> {
        @Override
        public WBCountry deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            final JsonObject object = element.getAsJsonObject();
            final String id = object.get("id").getAsString().trim();
            final String isoCode = object.get("iso2Code").getAsString().trim();
            final String name = object.get("name").getAsString().trim();
            final WBCity capitalCity = context.deserialize(element, WBCity.class);
            final WBRegion region = context.deserialize(object.getAsJsonObject("region"), WBRegion.class);
            final WBIncomeLevel incomeLevel = context.deserialize(object.getAsJsonObject("incomeLevel"), WBIncomeLevel.class);
            final WBLendingType lendingType = context.deserialize(object.getAsJsonObject("lendingType"), WBLendingType.class);
            return new WBCountry(id, isoCode, name, region, capitalCity, lendingType, incomeLevel);
        }
    }

}
