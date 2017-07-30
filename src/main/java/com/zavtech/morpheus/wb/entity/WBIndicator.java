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
import java.util.List;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;

/**
 * A class that represents a World Bank indicator
 *
 * @see <a href="http://data.worldbank.org/indicator">Indicator Search</a>
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-api-indicator-queries">World Bank API</a>
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBIndicator implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private WBSource source;
    private String sourceOrg;
    private String sourceNote;
    private List<WBTopic> topics;

    /**
     * Constructor
     * @param id            the id for this indicator
     * @param name          the name for this indicator
     * @param source        the source definition
     * @param sourceOrg     the source organization name
     * @param sourceNote    an optional note about source
     * @param topics        the list of topics for this indicator
     */
    public WBIndicator(String id, String name, WBSource source, String sourceOrg, String sourceNote, List<WBTopic> topics) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.sourceOrg = sourceOrg;
        this.sourceNote = sourceNote;
        this.topics = topics;
    }


    /**
     * Returns an indicator definition for the ticker specified
     * @param indicator     the indicator ticker value
     * @return              the indicator definition
     */
    public static Optional<WBIndicator> getIndicator(String indicator) {
        final WBLoader loader = new WBLoader();
        final String url = String.format("http://api.worldbank.org/indicators/%s?format=json", indicator);
        return loader.load(url, reader -> {
            try {
                final Gson gson = loader.builder().create();
                final List<WBIndicator> results = new ArrayList<>(20000);
                while (reader.hasNext()) {
                    final WBIndicator result = gson.fromJson(reader, WBIndicator.class);
                    if (indicator != null) {
                        results.add(result);
                    }
                }
                if (results.size() > 0) {
                    return Optional.of(results.iterator().next());
                } else {
                    return Optional.<WBIndicator>empty();
                }
            } catch (Exception ex) {
                throw new WBException("Failed to extract indicator records for " + url, ex);
            }
        }).getBody();
    }


    /**
     * Returns a list of all World Bank defined indicators
     * @return      the list of all World Bank indicators
     * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-api-indicator-queries">World Bank API</a>
     */
    public static List<WBIndicator> getIndicators() {
        final WBLoader loader = new WBLoader();
        final String url = "http://api.worldbank.org/indicators?format=json&per_page=30000";
        return loader.load(url, reader -> {
            try {
                final Gson gson = loader.builder().create();
                final List<WBIndicator> results = new ArrayList<>(20000);
                while (reader.hasNext()) {
                    final WBIndicator indicator = gson.fromJson(reader, WBIndicator.class);
                    if (indicator != null) {
                        results.add(indicator);
                    }
                }
                return results;
            } catch (Exception ex) {
                throw new WBException("Failed to extract indicator records for " + url, ex);
            }
        }).getBody();
    }

    /**
     * Returns the id for this indicator
     * @return      the id for this indicator
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the name for for this indicator
     * @return      the name for this indicator
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the source definition for this indicator
     * @return      the source definition
     */
    public WBSource getSource() {
        return source;
    }

    /**
     * Returns the source organization name for this indicator
     * @return      the source orgnaization name
     */
    public String getSourceOrg() {
        return sourceOrg;
    }

    /**
     * Returns the source notes for indicator
     * @return      the source notes
     */
    public String getSourceNote() {
        return sourceNote;
    }

    /**
     * Returns the list of topics for for this indicator
     * @return      the list of topics
     */
    public List<WBTopic> getTopics() {
        return topics != null ? Collections.unmodifiableList(topics) : Collections.emptyList();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBIndicator)) return false;
        final WBIndicator that = (WBIndicator)o;
        return getId().equals(that.getId());

    }


    @Override
    public int hashCode() {
        return getId().hashCode();
    }


    @Override
    public String toString() {
        return "WBIndicator{id='" + id + '\'' + ", name='" + name + '\'' + ", source=" + source + ", sourceOrg='" + sourceOrg + '\'' + '}';
    }

    /**
     * A deserializer for WBIndicator objects
     */
    public static class Deserializer implements JsonDeserializer<WBIndicator> {
        private final static Type topicListType = new TypeToken<List<WBTopic>>() {}.getType();
        @Override
        public WBIndicator deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            try {
                final JsonObject object = element.getAsJsonObject();
                final String id = object.get("id").getAsString();
                final String name = object.get("name").getAsString();
                final WBSource source = context.deserialize(object.get("source"), WBSource.class);
                final JsonElement sourceOrgElement = object.getAsJsonPrimitive("sourceOrganization");
                final JsonElement sourceNotesElement = object.getAsJsonPrimitive("sourceNote");
                final String sourceOrg = sourceOrgElement == null || sourceOrgElement.isJsonNull() ? null : sourceOrgElement.getAsString();
                final String sourceNotes = sourceNotesElement == null || sourceNotesElement.isJsonNull() ? null : sourceNotesElement.getAsString();
                final List<WBTopic> topics = context.deserialize(object.get("topics"), topicListType);
                return new WBIndicator(id, name, source, sourceOrg, sourceNotes, topics);
            } catch (Exception ex) {
                throw new WBException("Failed to deserialize WBIndicator", ex);
            }
        }
    }

}
