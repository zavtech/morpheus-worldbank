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

import java.util.function.Consumer;

import com.google.gson.Gson;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.wb.WBException;
import com.zavtech.morpheus.wb.WBLoader;

/**
 * A Morpheus DataSource that provides access to the World Bank Data Catalog
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902049-data-catalog-api">World Bank Catalog API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCatalogSource extends DataFrameSource<Integer,String,WBCatalogSource.Options> {


    /**
     * Constructor
     */
    public WBCatalogSource() {
        super();
    }


    @Override
    public DataFrame<Integer, String> read(Consumer<Options> configurator) throws DataFrameException {
        final WBLoader loader = new WBLoader();
        final String url = "http://api.worldbank.org/v2/datacatalog?format=json&per_page=1000";
        return loader.load(url, reader -> {
            try {
                final Gson gson = loader.builder().create();
                final DataFrame<Integer,String> result = DataFrame.empty();
                while (reader.hasNext()) {
                    reader.beginObject();


                    reader.endObject();
                }
                return result;
            } catch (Exception ex) {
                throw new WBException("Failed to extract World Bank Catalog records for " + url, ex);
            }
        }).getBody();
    }


    public static class Options implements DataFrameSource.Options<Integer,String> {


        @Override
        public void validate() {

        }
    }
}
