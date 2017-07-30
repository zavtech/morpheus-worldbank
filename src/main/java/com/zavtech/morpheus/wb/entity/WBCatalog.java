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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;

/**
 * A class that represents the World Bank Data catalog information
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902049-data-catalog-api">World Bank Catalog API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCatalog implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private List<Item> itemList = new ArrayList<>();
    private Map<Integer,Item> itemMap = new HashMap<>();

    /**
     * Constructor
     */
    public WBCatalog() {
        super();
    }

    /**
     * Adds an item to this catalog
     * @param item  the item to add
     */
    void addItem(Item item) {
        this.itemList.add(item);
        this.itemMap.put(item.getId(), item);
    }

    /**
     * Returns the list of items in the catalog
     * @return  the list of catalog items
     */
    public List<Item> getItems() {
        return Collections.unmodifiableList(itemList);
    }

    /**
     * Returns the item for the id specified
     * @param id    the id for item
     * @return      the optional match
     */
    public Optional<Item> getItem(int id) {
        return Optional.ofNullable(itemMap.get(id));
    }

    /**
     * Returns the world bank Catalog with all items
     * @return      the World Bank data catalog
     */
    public static WBCatalog getCatalog() {
        final WBLoader loader = new WBLoader(WBCatalog.class);
        final String url = "http://api.worldbank.org/v2/datacatalog?format=json&per_page=1000";
        return loader.load(url, reader -> {
            try {
                final Gson gson = loader.builder().create();
                final WBCatalog catalog = new WBCatalog();
                while (reader.hasNext()) {
                    final Item item = gson.fromJson(reader, Item.class);
                    if (item != null) {
                        catalog.addItem(item);
                    }
                }
                return catalog;
            } catch (Exception ex) {
                throw new WBException("Failed to extract World Bank Catalog records for " + url, ex);
            }
        }).getBody();
    }



    /**
     * A deserializer for WBCatalog.Item objects
     */
    public static class ItemDeserializer implements JsonDeserializer<Item> {
        @Override
        public Item deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            try {
                final Item item = new Item();
                final JsonObject object = element.getAsJsonObject();
                item.id = Integer.parseInt(object.get("id").getAsString());
                final JsonArray metaType = object.getAsJsonArray("metatype");
                final Matcher urlMatch = Pattern.compile("(.+)=(http.+$)").matcher("");
                final Matcher dateMatch = Pattern.compile("(\\d{2}-(.{3})-(\\d{4}))").matcher("");
                final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
                for (int i=0; i<metaType.size(); ++i) {
                    final JsonObject entry = metaType.get(i).getAsJsonObject();
                    final String entryName = entry.get("id").getAsString();
                    final String entryValue = entry.get("value").getAsString();
                    if (entryValue != null && entryValue.trim().length() > 0) {
                        if (entryName.equalsIgnoreCase("name")) {
                            item.name = entryValue;
                        } else if (entryName.equalsIgnoreCase("acronym")) {
                            item.acronym = entryValue;
                        } else if (entryName.equalsIgnoreCase("description")) {
                            item.description = entryValue;
                        } else if (entryName.equalsIgnoreCase("url")) {
                            item.url = entryValue;
                        } else if (entryName.equalsIgnoreCase("type")) {
                            item.type = entryValue;
                        } else if (entryName.equalsIgnoreCase("sourceurl")) {
                            item.sourceUrl = entryValue;
                        } else if (entryName.equalsIgnoreCase("listofcountriesregionssubnationaladmins")) {
                            item.regionalAdmins = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("languagesupported")) {
                            item.languagesSupported = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("periodicity")) {
                            item.periodicity = entryValue;
                        } else if (entryName.equalsIgnoreCase("economycoverage")) {
                            item.economyCoverage = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("granularity")) {
                            item.granularity = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("numberofeconomies")) {
                            item.numberOfEconomies = entryValue;
                        } else if (entryName.equalsIgnoreCase("topics")) {
                            item.topics = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("updatefrequency")) {
                            item.updateFrequency = entryValue;
                        } else if (entryName.equalsIgnoreCase("updateschedule")) {
                            item.updateSchedule = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("lastrevisiondate") && entryValue.equalsIgnoreCase("Current")) {
                            item.lastRevisionDate = LocalDate.now();
                        } else if (entryName.equalsIgnoreCase("lastrevisiondate") && dateMatch.reset(entryValue).matches()) {
                            item.lastRevisionDate = LocalDate.parse(entryValue, dateFormatter);
                        } else if (entryName.equalsIgnoreCase("contactdetails")) {
                            item.contactDetails = entryValue;
                        } else if (entryName.equalsIgnoreCase("accessoption")) {
                            item.accessOptions = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("bulkdownload")) {
                            item.bulkDownload = Arrays.stream(entryValue.split(";")).map(v -> urlMatch.reset(v).matches() ? urlMatch.group(2) : v).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("cite")) {
                            item.cite = Arrays.stream(entryValue.split(",")).map(String::trim).collect(Collectors.toSet());
                        } else if (entryName.equalsIgnoreCase("detailpageurl")) {
                            item.detailsPageUrl = entryValue;
                        } else if (entryName.equalsIgnoreCase("popularity")) {
                            item.popularity = Integer.parseInt(entryValue);
                        } else if (entryName.equalsIgnoreCase("coverage")) {
                            item.coverage = entryValue;
                        } else if (entryName.equalsIgnoreCase("api")) {
                            item.apiEnabled = Integer.parseInt(entryValue) == 1;
                        } else if (entryName.equalsIgnoreCase("apiaccessurl")) {
                            item.apiAccessUrl = entryValue;
                        } else if (entryName.equalsIgnoreCase("apisourceid")) {
                            item.apiSourceId = entryValue;
                        } else if (entryName.equalsIgnoreCase("dataNotes")) {
                            item.dataNotes = entryValue;
                        }
                    }
                }
                return item;
            } catch (Exception ex) {
                throw new WBException("Failed to deserialize WBCatalog Item", ex);
            }
        }
    }


    public static void main(String[] args) {
        final WBCatalog catalog = WBCatalog.getCatalog();
        final Set<String> values = catalog.getItems().stream().map(Item::getType).collect(Collectors.toSet());
        values.forEach(System.out::println);
    }


    /**
     * Represents an item within the Catalog.
     */
    public static class Item implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private int id;
        private String name;
        private String acronym;
        private String description;
        private String url;
        private String type;
        private String sourceUrl;
        private Set<String> regionalAdmins;
        private Set<String> languagesSupported;
        private String periodicity;
        private Set<String> economyCoverage;
        private Set<String> granularity;
        private String numberOfEconomies;
        private Set<String> topics;
        private String updateFrequency;
        private Set<String> updateSchedule;
        private LocalDate lastRevisionDate;
        private String contactDetails;
        private Set<String> accessOptions;
        private Set<String> bulkDownload;
        private Set<String> cite;
        private String detailsPageUrl;
        private int popularity;
        private String coverage;
        private boolean apiEnabled;
        private String apiAccessUrl;
        private String apiSourceId;
        private String dataNotes;


        /**
         * Returns the unique id for this item
         * @return      the unique id
         */
        public int getId() {
            return id;
        }

        /**
         * Returns the name for this dataset
         * @return  the dataset name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the dataset acronym
         * @return  the dataset acronym
         */
        public String getAcronym() {
            return acronym;
        }

        /**
         * Returns the dataset description if available
         * @return  the dataset description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Returns the World Bank url for this dataset
         * @return  the World Bank dataset url
         */
        public String getUrl() {
            return url;
        }

        /**
         * Returns the type for this dataset
         * @return      the dataset type
         */
        public String getType() {
            return type;
        }

        /**
         * Returns the source url for this dataset
         * @return      the source url
         */
        public String getSourceUrl() {
            return sourceUrl;
        }

        /**
         * Returns the list of supported languages
         * @return      the list of supported languages
         */
        public Set<String> getLanguagesSupported() {
            return languagesSupported  != null ? languagesSupported : Collections.emptySet();
        }

        /**
         * Returns the country or regional admins for this dataset
         * @return      the country or regional admins
         */
        public Set<String> getRegionalAdmins() {
            return regionalAdmins != null ? regionalAdmins : Collections.emptySet();
        }

        /**
         * Returns the dataset periodicity
         * @return  the dataset periodicity
         */
        public String getPeriodicity() {
            return periodicity;
        }

        /**
         * Returns the list of economies covered by this dataset
         * @return      the list of economies
         */
        public Set<String> getEconomyCoverage() {
            return economyCoverage  != null ? economyCoverage : Collections.emptySet();
        }

        /**
         * Returns the granularity levels for this dataset
         * @return      the granularity levels
         */
        public Set<String> getGranularity() {
            return granularity;
        }

        /**
         * Returns the number of economies covered by this dataset
         * @return      the number of economies covered
         */
        public String getNumberOfEconomies() {
            return numberOfEconomies;
        }

        /**
         * Returns the topic names for this dataset
         * @return      the topic names
         */
        public Set<String> getTopics() {
            return topics != null ? topics : Collections.emptySet();
        }

        /**
         * Returns the update frequency for this dataset
         * @return      the update frequency
         */
        public String getUpdateFrequency() {
            return updateFrequency;
        }

        /**
         * Returns the update schedule for this dataset
         * @return      the update schedule for dataset
         */
        public Set<String> getUpdateSchedule() {
            return updateSchedule != null ? updateSchedule : Collections.emptySet();
        }

        /**
         * Returns the last revision date for this dataset
         * @return      the last revision date
         */
        public LocalDate getLastRevisionDate() {
            return lastRevisionDate;
        }

        /**
         * Returns the contact details for this dataset, usually an email address
         * @return      the contact details
         */
        public String getContactDetails() {
            return contactDetails;
        }

        /**
         * Returns the access options for this dataset
         * @return      the access options
         */
        public Set<String> getAccessOptions() {
            return accessOptions != null ? accessOptions : Collections.emptySet();
        }

        /**
         * Returns the bulk download urls for this dataset in one or more formats
         * @return      the bulk download urls
         */
        public Set<String> getBulkDownload() {
            return bulkDownload != null ? bulkDownload : Collections.emptySet();
        }

        /**
         * Returns the set of citations for this dataset
         * @return  the set of citations
         */
        public Set<String> getCite() {
            return cite  != null ? cite : Collections.emptySet();
        }

        /**
         * Returns the details page url for this dataset
         * @return      the details page url
         */
        public String getDetailsPageUrl() {
            return detailsPageUrl;
        }

        /**
         * Returns the popularity score for this dataset
         * @return      the popularity score
         */
        public int getPopularity() {
            return popularity;
        }

        /**
         * Returns the time period over which data is available
         * @return      the coverage
         */
        public String getCoverage() {
            return coverage;
        }

        /**
         * Returns true if this dataset is API enabled
         * @return  true if API enabled
         */
        public boolean isApiEnabled() {
            return apiEnabled;
        }

        /**
         * Returns the API access url or null if not available
         * @return  the API access url
         */
        public String getApiAccessUrl() {
            return apiAccessUrl;
        }

        /**
         * Returns the API source id
         * @return  the API source id
         */
        public String getApiSourceId() {
            return apiSourceId;
        }

        /**
         * Returns the notes associated with this dataset if any
         * @return  the notes for this dataset
         */
        public String getDataNotes() {
            return dataNotes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;
            final Item item = (Item) o;
            return getId() == item.getId();
        }

        @Override
        public int hashCode() {
            return getId();
        }

        @Override()
        public String toString() {
            return String.format("WBCatalog.Item{id=%s, name=%s, url=%s}", getId(), getName(), getUrl());
        }
    }

}
