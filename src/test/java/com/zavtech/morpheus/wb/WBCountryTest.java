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


import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.wb.entity.WBCountry;
import com.zavtech.morpheus.wb.source.WBCountrySource;

/**
 * A unit test for the World Bank Country source
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898590-api-country-queries">World Bank API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBCountryTest {

    static {
        DataFrameSource.register(new WBCountrySource());
    }


    @Test
    public void testList() {
        final List<WBCountry> countries = WBCountry.getCountries();
        final Map<String,WBCountry> countryMap1 = countries.stream().collect(Collectors.toMap(WBCountry::getId, c -> c));
        final Map<String,WBCountry> countryMap2 = countries.stream().collect(Collectors.toMap(WBCountry::getIso2Code, c -> c));
        Assert.assertTrue(countries.size() > 300, "There are at least 300 countries");
        Assert.assertEquals(countries.size(), countryMap1.size(), "All countries have a unique id");

        final WBCountry uk = countryMap2.get("GB");
        Assert.assertEquals(uk.getName(), "United Kingdom");
        Assert.assertTrue(uk.getCapitalCity().isPresent());
        Assert.assertTrue(uk.getRegion().isPresent());
        Assert.assertTrue(uk.getIncomeLevel().isPresent());
        Assert.assertTrue(uk.getLendingType().isPresent());
        Assert.assertEquals(uk.getCapitalCity().get().getName(), "London");
        Assert.assertEquals(uk.getCapitalCity().get().getLatitude(), "51.5002");
        Assert.assertEquals(uk.getCapitalCity().get().getLongitude(), "-0.126236");
        Assert.assertEquals(uk.getRegion().get().getId(), "ECS");
        Assert.assertEquals(uk.getRegion().get().getValue(), "Europe & Central Asia");
        Assert.assertEquals(uk.getIncomeLevel().get().getId(), "HIC");
        Assert.assertEquals(uk.getIncomeLevel().get().getValue(), "High income");
        Assert.assertEquals(uk.getLendingType().get().getId(), "LNX");
        Assert.assertEquals(uk.getLendingType().get().getValue(), "Not classified");

        final WBCountry za = countryMap2.get("ZA");
        Assert.assertEquals(za.getName(), "South Africa");
        Assert.assertTrue(za.getCapitalCity().isPresent());
        Assert.assertTrue(za.getRegion().isPresent());
        Assert.assertTrue(za.getIncomeLevel().isPresent());
        Assert.assertTrue(za.getLendingType().isPresent());
        Assert.assertEquals(za.getCapitalCity().get().getName(), "Pretoria");
        Assert.assertEquals(za.getCapitalCity().get().getLatitude(), "-25.746");
        Assert.assertEquals(za.getCapitalCity().get().getLongitude(), "28.1871");
        Assert.assertEquals(za.getRegion().get().getId(), "SSF");
        Assert.assertEquals(za.getRegion().get().getValue(), "Sub-Saharan Africa");
        Assert.assertEquals(za.getIncomeLevel().get().getId(), "UMC");
        Assert.assertEquals(za.getIncomeLevel().get().getValue(), "Upper middle income");
        Assert.assertEquals(za.getLendingType().get().getId(), "IBD");
        Assert.assertEquals(za.getLendingType().get().getValue(), "IBRD");
    }


    @Test
    public void testDataFrame() {
        final DataFrame<String,WBCountry.Field> frame = DataFrameSource.lookup(WBCountrySource.class).read(o -> o.setUseIsoCode(true));
        final List<WBCountry> countries = WBCountry.getCountries();
        final Map<String,WBCountry> countryMap = countries.stream().collect(Collectors.toMap(WBCountry::getIso2Code, c -> c));
        Assert.assertTrue(frame.rowCount() > 300, "There are at least 300 countries");
        Assert.assertEquals(frame.rowCount(), countries.size(), "Frame row count");
        Assert.assertEquals(frame.colCount(), WBCountry.Field.values().length-1);
        frame.out().print();

        frame.rows().forEach(row -> {
            final String isoCode = row.key();
            final WBCountry country = countryMap.get(isoCode);
            Assert.assertTrue(country != null, "Matched country for " + isoCode);
        });
    }
}
