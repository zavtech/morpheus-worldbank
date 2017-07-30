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

import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.wb.entity.WBCatalog;

/**
 * A unit test for the WBCatalog entity
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902049-data-catalog-api">World Bank Catalog API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCatalogTest {


    @Test()
    public void testCatalog() {
        final WBCatalog catalog = WBCatalog.getCatalog();
        Assert.assertTrue(catalog.getItems().size() > 200, "There are at least 200 items in the catalog");
        catalog.getItems().forEach(item -> {
            Assert.assertTrue(item.getId() > 0, "The id is > 0");
            Assert.assertNotNull(item.getName(), "The item name is defined");
        });

        final Optional<WBCatalog.Item> wdi = catalog.getItem(1);
        Assert.assertTrue(wdi.isPresent());
        wdi.ifPresent(item -> {
            Assert.assertEquals(item.getId(), 1);
            Assert.assertEquals(item.getName(), "World Development Indicators");
            Assert.assertTrue(item.getDescription().startsWith("The primary World Bank collection of development "));
            Assert.assertEquals(item.getUrl(), "http://databank.worldbank.org/data/views/variableSelection/selectvariables.aspx?source=world-development-indicators");
            Assert.assertEquals(item.getType(), "Time series");
            Assert.assertTrue(item.getLanguagesSupported().containsAll(Collect.asSet("English", "Spanish", "French", "Arabic", "Chinese")));
            Assert.assertEquals(item.getPeriodicity(), "Annual");
            Assert.assertTrue(item.getEconomyCoverage().containsAll(Collect.asSet("WLD", "EAP", "ECA", "LAC", "MNA", "SAS", "SSA", "HIC", "LMY", "IBRD", "IDA")));
            Assert.assertTrue(item.getGranularity().containsAll(Collect.asSet("National", "Regional")));
            Assert.assertTrue(Integer.parseInt(item.getNumberOfEconomies()) >= 217);
            Assert.assertTrue(item.getTopics().containsAll(Collect.asSet("Agriculture & Rural Development", "Aid Effectiveness", "Climate Change", "Economy & Growth")));
            Assert.assertEquals(item.getUpdateFrequency(), "Quarterly");
            Assert.assertTrue(item.getUpdateSchedule().containsAll(Collect.asSet("April", "July", "September", "December")));
            Assert.assertNotNull(item.getLastRevisionDate());
            Assert.assertEquals(item.getContactDetails(), "data@worldbank.org");
            Assert.assertTrue(item.getAccessOptions().containsAll(Collect.asSet("API", "Bulk download", "Query tool")));
            Assert.assertTrue(item.getCite().containsAll(Collect.asSet("World Development Indicators", "The World Bank")));
            Assert.assertEquals(item.getDetailsPageUrl(), "http://data.worldbank.org/data-catalog/world-development-indicators");
            Assert.assertEquals(item.getApiAccessUrl(), "http://data.worldbank.org/developers");
            Assert.assertEquals(item.getApiSourceId(), "2");
            Assert.assertTrue(item.getBulkDownload().containsAll(Collect.asSet(
                    "http://databank.worldbank.org/data/download/WDI_excel.zip=excel",
                    "http://databank.worldbank.org/data/download/WDI_csv.zip=csv",
                    "http://databank.worldbank.org/data/download/WDIrevisions.xls=excel"
            )));
        });
    }
}
