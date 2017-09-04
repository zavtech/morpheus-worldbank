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
package com.zavtech.morpheus.wb.source;

import java.time.LocalDate;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Initialiser;
import com.zavtech.morpheus.util.text.parser.Parser;
import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;
import com.zavtech.morpheus.wb.WBResponse;

/**
 * A data source implementation that returns World Bank Indicator definitions as well as Indicator time series data.
 *
 * @see <a href="http://data.worldbank.org/indicator">Indicator Search</a>
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-api-indicator-queries">World Bank API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBIndicatorSource extends DataFrameSource<LocalDate,String,WBIndicatorSource.Options> {


    /**
     * Constructor
     */
    public WBIndicatorSource() {
        super();
    }


    @Override
    public DataFrame<LocalDate,String> read(Consumer<Options> handler) throws DataFrameException {
        final Options options = Initialiser.apply(new Options(), handler);
        if (options.indicator == null) {
            throw new WBException("An indicator ticker must be specified when querying for indicator values");
        } else {
            final String indicator = options.indicator;
            final LocalDate start = Optional.ofNullable(options.startDate).orElse(LocalDate.of(1970, 1, 1));
            final LocalDate end = Optional.ofNullable(options.endDate).orElse(LocalDate.now());
            final Range<LocalDate> dateRange = Range.of(start, end);
            if (options.countries.isEmpty()) {
                return getIndicatorValues(indicator, dateRange, "all", options.batchSize);
            } else {
                return DataFrame.combineFirst(options.countries.stream().map(country -> {
                    return getIndicatorValues(indicator, dateRange, country, options.batchSize);
                }));
            }
        }
    }


    /**
     * Returns a DataFrame of indicator values given the ticker and date range specified
     * @param indicator     the World Bank indicator ticker
     * @param dateRange     the date range for request
     * @param batchSize     the batch size for each request
     * @param country       the country ISO2 code to query (e.g. US=United States, GB=United Kingdom)
     * @return              the DataFrame with dates on the row axis, country along the columns
     * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-api-indicator-queries">World Bank API</a>
     */
    private DataFrame<LocalDate,String> getIndicatorValues(String indicator, Range<LocalDate> dateRange, String country, int batchSize) {
        final WBLoader loader = new WBLoader();
        final String peekUrl = createURL(indicator, dateRange, country, 1, 5);
        final Function<JsonReader,DataFrame<LocalDate,String>> handler = createMessageHandler(indicator);
        final WBResponse<DataFrame<LocalDate,String>> peek = loader.load(peekUrl, handler);
        final int requestCount = peek.getRequestCount(batchSize);
        final IntStream pages = requestCount > 10 ? IntStream.range(0, requestCount).parallel() : IntStream.range(0, requestCount);
        return DataFrame.combineFirst(pages.mapToObj(i -> {
            final int page = i + 1;
            final String url = createURL(indicator, dateRange, country, page, batchSize);
            final WBResponse<DataFrame<LocalDate,String>> response = loader.load(url, handler);
            return response.getBody();
        }));
    }



    /**
     * Returns the query URL based on the date range and the set of countries
     * @param indicator     the ticker for indicator
     * @param range         the date range
     * @param country       the country ticker, "all" for all countries
     * @return              returns the World Bank indicator query URL
     */
    private String createURL(String indicator, Range<LocalDate> range, String country, int page, int perPage) {
        final int startYear = range.start().getYear();
        final int endYear = range.end().getYear();
        final String template = "http://api.worldbank.org/countries/%s/indicators/%s?date=%s:%s&format=json&page=%s&per_page=%s";
        return String.format(template, country, indicator, startYear, endYear, page, perPage);
    }


    /**
     * Returns a newly created message handler to process indicator data results
     * @param indicator     the indicator ticker
     * @return              the message handler
     */
    private Function<JsonReader,DataFrame<LocalDate,String>> createMessageHandler(String indicator) {
        return reader -> {
            try {
                if (reader.peek() == JsonToken.NULL) {
                    return DataFrame.empty();
                }
                String name = null;
                String country = null;
                String date = null;
                String value = null;
                final Parser<Double> doubleParser = Parser.ofDouble();
                final Index<LocalDate> rowKeys = Index.of(LocalDate.class, 5000);
                final Index<String> colKeys = Index.of(String.class, 1000);
                DataFrame<LocalDate,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);
                while (reader.hasNext()) {
                    final JsonToken jsonToken = reader.peek();
                    if (jsonToken == JsonToken.BEGIN_OBJECT) {
                        reader.beginObject();
                    } else if (jsonToken == JsonToken.END_OBJECT) {
                        reader.endObject();
                    } else if (jsonToken == JsonToken.BEGIN_ARRAY) {
                        reader.beginArray();
                    } else if (jsonToken == JsonToken.END_ARRAY) {
                        reader.endArray();
                    } else {
                        name = reader.nextName();
                        if (reader.peek() == JsonToken.NULL) {
                            reader.nextNull();
                        } else if (name.equalsIgnoreCase("indicator")) {
                            reader.skipValue();
                        } else if (name.equalsIgnoreCase("country")) {
                            reader.beginObject();
                            for (int i=0; i<2; ++i) {
                                if (reader.nextName().equalsIgnoreCase("id")) {
                                    country = reader.nextString();
                                } else {
                                    reader.skipValue();
                                }
                            }
                            reader.endObject();;
                        } else if (name.equalsIgnoreCase("value") && reader.peek() == JsonToken.NULL) {
                            reader.nextNull();
                        } else if (name.equalsIgnoreCase("value")) {
                            value =  reader.nextString();
                        } else if (name.equalsIgnoreCase("decimal")) {
                            reader.skipValue();
                        } else if (name.equalsIgnoreCase("date")) {
                            date = reader.nextString();
                        } else {
                            throw new WBException("Unexpected token value in JSON: " + name);
                        }
                        if (reader.peek() == JsonToken.END_OBJECT) {
                            reader.endObject();
                            if (date != null && country != null) {
                                if (!date.equalsIgnoreCase("MRV")) {
                                    final int year = Integer.parseInt(date);
                                    final LocalDate localDate = LocalDate.of(year, 12, 31);
                                    final double indicatorValue = doubleParser.apply(value);
                                    frame.rows().add(localDate);
                                    frame.cols().add(country, Double.class);
                                    frame.data().setDouble(localDate, country, indicatorValue);
                                    name = null; date = null; value = null; country = null;
                                }
                            }
                        }
                    }
                }
                return frame;
            } catch (Exception ex) {
                throw new WBException("Failed to extract indicator values for " + indicator, ex);
            }
        };
    }


    /**
     * An options definition for the WBIndicatorSource
     */
    public static class Options implements DataFrameSource.Options<LocalDate,String> {

        private String indicator;
        private int batchSize = 10000;
        private LocalDate startDate;
        private LocalDate endDate;
        private Set<String> countries;

        /**
         * Constructor
         */
        public Options() {
            this.batchSize = 10000;
            this.countries = new TreeSet<>();
            this.startDate = LocalDate.of(1970, 1, 1);
            this.endDate = LocalDate.now();
        }

        /**
         * Sets the World Bank indicator to query for
         * @param indicator     the indicator ticker
         */
        public void setIndicator(String indicator) {
            this.indicator = indicator;
        }

        /**
         * Sets the start date for these options
         * @param startDate     the start date
         */
        public void setStartDate(LocalDate startDate) {
            this.startDate = startDate;
        }

        /**
         * Sets the start date for these options
         * @param endDate       sets the end date
         */
        public void setEndDate(LocalDate endDate) {
            this.endDate = endDate;
        }

        /**
         * Sets the batch size for these options
         * @param batchSize     the batch size
         */
        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        /**
         * Sets all countries for these options
         */
        public void setAllCountries() {
            this.countries.clear();
            this.countries.add("all");
        }

        /**
         * Sets a single country for these options
         * @param country the ISO2 country code
         */
        public void setCountry(String country) {
            this.countries.clear();
            if (country != null) {
                this.countries.add(country);
            }
        }

        /**
         * Sets multiple countries for these options
         * @param countries the ISO2 country codes
         */
        public void setCountries(String... countries) {
            this.countries.clear();
            for (String country : countries) {
                if (country != null) {
                    this.countries.add(country);
                }
            }
        }

        /**
         * Sets multiple countries for these options
         * @param countries the ISO2 country codes
         */
        public void setCountries(Iterable<String> countries) {
            this.countries.clear();
            countries.forEach(country -> {
                if (country != null) {
                    this.countries.add(country);
                }
            });
        }

        @Override
        public void validate() {

        }
    }



    public static void main(String[] args) throws Exception {
        final WBIndicatorSource source = new WBIndicatorSource();
        final DataFrame<LocalDate,String> frame = source.read(options -> {
            options.setIndicator("NY.GDP.PCAP.CD");
            options.setBatchSize(1000);
            options.setStartDate(LocalDate.of(1970, 1, 1));
            options.setEndDate(LocalDate.now());
            options.setCountries("JP", "US", "DE", "IT", "GB", "FR", "CA");
        });

        frame.out().print(200);

        Thread.currentThread().join();
    }


}
