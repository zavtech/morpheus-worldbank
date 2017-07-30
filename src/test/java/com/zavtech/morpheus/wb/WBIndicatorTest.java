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

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.wb.entity.WBIndicator;
import com.zavtech.morpheus.wb.source.WBIndicatorSource;

/**
 * A unit test for the World Bank Indicator source
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-api-indicator-queries">World Bank API</a>
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBIndicatorTest {

    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new WBIndicatorSource());
    }


    @Test()
    public void testLoadIndicators() {
        final List<WBIndicator> indicators = WBIndicator.getIndicators();
        final Map<String,WBIndicator> indicatorMap = new HashMap<>(indicators.size());
        indicators.forEach(indicator -> indicatorMap.put(indicator.getId(), indicator));
        Assert.assertTrue(indicators.size() > 15000, "There are at least 15K indicators");
        Assert.assertTrue(indicatorMap.size() > 15000, "All indicators have a unique key");
    }


    @Test()
    public void testC02() {
        final DataFrame<LocalDate,String> expected = DataFrame.read().csv(options -> {
            options.setResource("/indicators/EN.ATM.CO2E.PC.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setExcludeColumnIndexes(0);
        });
        final DataFrame<LocalDate,String> actual = DataFrameSource.lookup(WBIndicatorSource.class).read(options -> {
            options.setBatchSize(1000);
            options.setIndicator("EN.ATM.CO2E.PC");
            options.setStartDate(LocalDate.of(1970, 1, 1));
            options.setEndDate(LocalDate.of(2013, 1, 1));
            options.setCountries("JP", "US", "DE", "IT", "GB", "FR", "CA", "CN");
        });
        expected.out().print(100);
        actual.out().print(100);
        DataFrameAsserts.assertEqualsByIndex(actual, expected);
    }



    @Test(enabled = false)
    public void testAll() {
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setParallel(true);
            options.setResource("/Users/witdxav/Dropbox/data/world-bank/WDI/WDIData.csv");
        });
        frame.out().print();

        final Set<String> indicatorSet = new HashSet<>();
        frame.rows().forEach(row -> {
            final long nonNullCount = row.values().filter(v -> !v.isNull()).count();
            if (nonNullCount > 0) {
                final String indicator = row.getValue("Indicator Code");
                if (indicator != null) {
                    indicatorSet.add(indicator);
                }
            }
        });

        indicatorSet.forEach(indicator -> {
            System.out.println("Loading data for indicator: " + indicator);
            DataFrameSource.lookup(WBIndicatorSource.class).read(options -> {
                options.setBatchSize(10000);
                options.setIndicator(indicator);
                options.setStartDate(LocalDate.of(2014, 1, 1));
                options.setEndDate(LocalDate.of(2015, 1, 1));
            }).out().print();
        });
    }
}
