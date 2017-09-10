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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import com.zavtech.morpheus.util.http.HttpClient;
import com.zavtech.morpheus.wb.entity.WBCatalog;
import com.zavtech.morpheus.wb.entity.WBCity;
import com.zavtech.morpheus.wb.climate.WBClimate;
import com.zavtech.morpheus.wb.entity.WBCountry;
import com.zavtech.morpheus.wb.entity.WBIncomeLevel;
import com.zavtech.morpheus.wb.entity.WBIndicator;
import com.zavtech.morpheus.wb.entity.WBLendingType;
import com.zavtech.morpheus.wb.entity.WBRegion;
import com.zavtech.morpheus.wb.entity.WBSource;
import com.zavtech.morpheus.wb.entity.WBTopic;

/**
 * A helper class used to load JSON content from the World Bank
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBLoader {

    private Class<?> type;

    /**
     * Constructor
     */
    public WBLoader() {
        this(null);
    }


    /**
     * Constructor
     * @param type  the type for this loader
     */
    public WBLoader(Class<?> type) {
        this.type = type;
    }


    /**
     * A generic method to load JSON content for various World Bank API calls
     * @param url               the World Bank API get request URL
     * @param messageHandler    the function that turns JSON into some object representation
     * @param <T>               the data type for the response message
     * @return                  the resulting response
     * @throws WBException      if there is some access processing the request
     */
    public <T> WBResponse<T> load(String url, Function<JsonReader,T> messageHandler) throws WBException {
        return HttpClient.getDefault().<WBResponse<T>>doGet(request -> {
            final long t1 = System.currentTimeMillis();
            request.setUrl(url);
            request.setRetryCount(3);
            request.setResponseHandler(response -> {
                try {
                    if (response.getStatus().getCode() == 200) {
                        final Gson gson = builder().create();
                        final JsonReader reader = createReader(response.getStream());
                        final WBHeader header = parseHeader(reader, gson);
                        final T body = messageHandler.apply(reader);
                        final long t2 = System.currentTimeMillis();
                        System.out.println("World Bank request " + url + " completed in " + (t2-t1) + " millis");
                        return Optional.of(new WBResponse<>(header, body));
                    } else {
                        throw new WBException("World Bank API responded with status code: " + response.getStatus() + " to " + url);
                    }
                } catch (WBException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new WBException("", ex);
                }
            });
        }).orElseThrow(() -> {
            return new WBException("No data generated from World Bank APi request: " + url);
        });
    }


    /**
     * Parses the header in the JSON message
     * @param reader        the JSON reader
     * @param gson          the GSON engine
     * @return              the header for message
     * @throws IOException  if I/O exception
     */
    private WBHeader parseHeader(JsonReader reader, Gson gson) throws IOException {
        if (type == WBCatalog.class) {
            int pageNumber = -1;
            int pageCount = -1;
            String recordCount = "0";
            int totalRecords = -1;
            final AtomicInteger itemCount = new AtomicInteger();
            reader.beginObject();
            while (itemCount.get() < 5) {
                itemCount.incrementAndGet();
                final String name = reader.nextName().toLowerCase();
                switch (name) {
                    case "page":        pageNumber = reader.nextInt();      break;
                    case "pages":       pageCount = reader.nextInt();       break;
                    case "per_page":    recordCount = reader.nextString();  break;
                    case "total":       totalRecords = reader.nextInt();    break;
                    case "datacatalog": reader.beginArray();                break;
                    default: throw new IllegalStateException("Unexpected attribute in json: " + name);
                }
            }
            return new WBHeader(pageNumber, pageCount, Integer.parseInt(recordCount), totalRecords);
        } else {
            if (reader.peek() == JsonToken.NULL) {
                return gson.fromJson(reader, WBHeader.class);
            } else {
                reader.beginArray();
                final WBHeader header = gson.fromJson(reader, WBHeader.class);
                if (reader.peek() == JsonToken.BEGIN_ARRAY) {
                    reader.beginArray();
                }
                return header;
            }
        }
    }


    /**
     * Returns a newly created GsonBuilder initialized with World Bank Serializers
     * @return      the newly created Gson Builder
     */
    public GsonBuilder builder() {
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(WBTopic.class, new WBTopic.Deserializer());
        builder.registerTypeAdapter(WBSource.class, new WBSource.Deserializer());
        builder.registerTypeAdapter(WBIndicator.class, new WBIndicator.Deserializer());
        builder.registerTypeAdapter(WBCity.class, new WBCity.Deserializer());
        builder.registerTypeAdapter(WBRegion.class, new WBRegion.Deserializer());
        builder.registerTypeAdapter(WBLendingType.class, new WBLendingType.Deserializer());
        builder.registerTypeAdapter(WBIncomeLevel.class, new WBIncomeLevel.Deserializer());
        builder.registerTypeAdapter(WBCountry.class, new WBCountry.Deserializer());
        builder.registerTypeAdapter(WBHeader.class, new WBHeader.Deserializer());
        builder.registerTypeAdapter(WBCatalog.Item.class, new WBCatalog.ItemDeserializer());
        builder.registerTypeAdapter(WBClimate.MonthlyRecord.class, new WBClimate.MonthlyRecordDeserializer());
        return builder;
    }


    /**
     * Returns a newly created JsonReader
     * @param is        the input stream
     * @return          the JsonReader
     * @throws IOException  if there is an I/O exception
     */
    public JsonReader createReader(InputStream is) throws IOException {
        final String encoding = "UTF-8";
        if (is instanceof BufferedInputStream) {
            return new JsonReader(new InputStreamReader(is, encoding));
        } else {
            return new JsonReader(new InputStreamReader(new BufferedInputStream(is), encoding));
        }
    }

}
