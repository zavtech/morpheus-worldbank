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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * A class that represents a World Bank source definition
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBSource implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private static List<WBSource> sourceList;

    private int id;
    private String name;
    private String url;
    private String description;

    /**
     * Constructor
     * @param id            the source id
     * @param name          the source name
     * @param url           the source url
     * @param description   the source description
     */
    public WBSource(int id, String name, String url, String description) {
        this.id = id;
        this.name = name;
        this.url = url;
        this.description = description;
    }

    /**
     * Returns a list of all World Bank defined sources
     * @return      the list of all World Bank sources
     */
    public static synchronized List<WBSource> getSources() {
        if (sourceList != null && sourceList.size() > 0) {
            return Collections.unmodifiableList(sourceList);
        } else {
            final WBLoader loader = new WBLoader();
            final String url = "http://api.worldbank.org/sources?format=json&per_page=1000";
            return Collections.unmodifiableList(sourceList = loader.load(url, reader -> {
                try {
                    final Gson gson = loader.builder().create();
                    final List<WBSource> results = new ArrayList<>(1000);
                    while (reader.hasNext()) {
                        WBSource source = gson.fromJson(reader, WBSource.class);
                        if (source != null) {
                            results.add(source);
                        }
                    }
                    return results;
                } catch (Exception ex) {
                    throw new WBException("Failed to extract source records for " + url, ex);
                }
            }).getBody());
        }
    }


    /**
     * Returns the source id
     * @return  the source id
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the source value
     * @return  the source value
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the source url
     * @return  the source url
     */
    public Optional<String> getUrl() {
        return Optional.ofNullable(url);
    }

    /**
     * Returns the source description
     * @return  the source description
     */
    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBSource)) return false;
        final WBSource source = (WBSource) o;
        return getId() == source.getId();

    }

    @Override
    public int hashCode() {
        return id;
    }


    @Override
    public String toString() {
        return String.format("WBSource{id=%s, name='%s', url=%s, description='%s'}", getId(), getName(), getUrl(), getDescription());
    }

    /**
     * A deserializer for WBRegion objects
     */
    public static class Deserializer implements JsonDeserializer<WBSource> {

        private Map<Integer,WBSource> sourceMap = new HashMap<>();

        /**
         * Constructor
         */
        public Deserializer() {
            if (sourceList != null && sourceList.size() > 0) {
                sourceList.forEach(source -> {
                    sourceMap.put(source.getId(), source);
                });
            }
        }

        @Override
        public WBSource deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            final JsonObject object = element.getAsJsonObject();
            final int id = Integer.parseInt(object.get("id").getAsString());
            if (object.has("name")) {
                final String name = object.get("name").getAsString();
                final String url = object.get("url").getAsString();
                final String description = object.get("description").getAsString();
                return new WBSource(id, name, url != null && url.trim().length() > 0 ? url : null, description);
            } else {
                final String name = object.get("value").getAsString();
                return new WBSource(id, name, "N/A", "N/A");
            }
        }
    }


    public static void main(String[] args) {
        final List<WBSource> sources = WBSource.getSources();
        sources.forEach(System.out::println);
        System.out.println("There are " + sourceList.size() + " available sources...");
    }

}
