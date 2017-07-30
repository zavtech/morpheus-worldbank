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

import java.time.Month;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.http.HttpClient;
import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;
import com.zavtech.morpheus.wb.climate.WBClimate;
import com.zavtech.morpheus.wb.climate.WBClimateKey;

/**
 * Class summary goes here...
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902061-climate-data-api">World Bank Climate API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBClimateSource extends DataFrameSource<WBClimateKey,Month,WBClimateSource.Options> {

    private static List<Range<Integer>> YEAR_RANGES = Collect.asList(
        Range.of(1920, 1939),
        Range.of(1940, 1959),
        Range.of(1960, 1979),
        Range.of(1980, 1999),
        Range.of(2020, 2039),
        Range.of(2040, 2059),
        Range.of(2060, 2079),
        Range.of(2080, 2099)
    );


    /**
     * Constructor
     */
    public WBClimateSource() {
        super();
    }


    @Override
    public DataFrame<WBClimateKey,Month> read(Consumer<Options> configurator) throws DataFrameException {
        try {
            final Options options = initOptions(new Options(), configurator);
            final Index<WBClimateKey> rowIndex = Index.of(WBClimateKey.class, 1000);
            final DataFrame<WBClimateKey,Month> result = DataFrame.ofDoubles(rowIndex, Array.of(Month.values()));
            YEAR_RANGES.forEach(range -> {
                HttpClient.getDefault().doGet(request -> {
                    final long t1 = System.currentTimeMillis();
                    final String url = createUrl(options, range);
                    request.setUrl(url);
                    request.setRetryCount(3);
                    request.setResponseHandler((status, stream) -> {
                        try {
                            if (status.getCode() == 200) {
                                final WBLoader loader = new WBLoader();
                                final Gson gson = loader.builder().create();
                                final JsonReader reader = loader.createReader(stream);
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    final WBClimate.MonthlyRecord record = gson.fromJson(reader, WBClimate.MonthlyRecord.class);
                                    if (record != null) {
                                        final WBClimateKey key = record.getKey();
                                        final Month[] months = Month.values();
                                        result.rows().add(key);
                                        for (int i=0; i<months.length; ++i) {
                                            final double value = record.getValues()[i];
                                            result.data().setDouble(key, months[i], value);
                                        }
                                    }
                                }
                                final long t2 = System.currentTimeMillis();
                                System.out.println("World Bank request " + url + " completed in " + (t2-t1) + " millis");
                                return Optional.empty();
                            } else {
                                throw new WBException("World Bank API responded with status code: " + status + " to " + url);
                            }
                        } catch (WBException ex) {
                            throw ex;
                        } catch (Exception ex) {
                            throw new WBException("", ex);
                        }
                    });
                });
            });
            return result;
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("World Bank climate API request failed", ex);
        }
    }


    /**
     * Returns the World Bank Climate API REST URL for the options and year range
     * @param options   the user specified options
     * @param range     the year range
     * @return          the REST url
     */
    private String createUrl(Options options, Range<Integer> range) {
        final String baseUrl = "http://climatedataapi.worldbank.org/climateweb/rest/v1/country/mavg";
        final String urlSuffix = String.format("%s/%s/%s/%s.json", options.variable.getCode(), range.start(), range.end(), options.countryCode);
        if (options.gcm != null && options.sres != null) {
            return String.format("%s/%s/%s/%s", baseUrl, options.gcm.getCode(), options.sres.getCode(), urlSuffix);
        } else if (options.gcm != null) {
            return String.format("%s/%s/%s", baseUrl, options.gcm.getCode(), urlSuffix);
        } else if (options.sres != null) {
            return String.format("%s/%s/%s", baseUrl, options.sres.getCode(), urlSuffix);
        } else {
            return String.format("%s/%s", baseUrl, urlSuffix);
        }
    }


    /**
     * Quick and dirty test of the source
     */
    public static void main(String[] args) {
        final WBClimateSource source = new WBClimateSource();
        final DataFrame<WBClimateKey,Month> frame = source.read(options -> {
            options.setVariable(WBClimate.Variable.PRECIPITATION);
            options.setCountryCode("USA");
            //options.setSres(WBClimate.SRES.A2);
            options.setGcm(WBClimate.GCM.BCM_2_0);
        });
        frame.out().print(100);
    }


    /**
     * The source options for the World Bank Climate API source
     */
    public static class Options implements DataFrameSource.Options<WBClimateKey,Month> {

        private String countryCode;
        private WBClimate.GCM gcm;
        private WBClimate.SRES sres;
        private WBClimate.Variable variable;

        /**
         * Sets the country code as an ISO3 string
         * @param countryCode   the country code
         */
        public void setCountryCode(String countryCode) {
            this.countryCode = countryCode;
        }

        /**
         * Sets the General Circulation Model to use, null to select all
         * @param gcm   the GCM model
         */
        public void setGcm(WBClimate.GCM gcm) {
            this.gcm = gcm;
        }

        /**
         * Sets the emissions scenario code, null for all
         * @param sres  the emissions scenario code
         */
        public void setSres(WBClimate.SRES sres) {
            this.sres = sres;
        }

        /**
         * Sets the variable to select, which is either precipitation or temperature
         * @param variable      the variable to select
         */
        public void setVariable(WBClimate.Variable variable) {
            this.variable = variable;
        }

        @Override
        public void validate() {
            if (countryCode == null) {
                throw new WBException("A country ISO 3 code must be specified");
            } else if (variable == null) {
                throw new WBException("A climate variable code must be specified");
            }
         }
    }


}
