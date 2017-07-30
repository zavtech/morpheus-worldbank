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
package com.zavtech.morpheus.wb.climate;

import java.time.Year;

import com.zavtech.morpheus.util.Asserts;

/**
 * Class summary goes here...
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902061-climate-data-api">World Bank Climate API</a>
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBClimateKey implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private Year start;
    private Year end;
    private WBClimate.GCM gcm;
    private WBClimate.SRES scenario;
    private WBClimate.Variable variable;

    /**
     * Constructor
     * @param start     the start year
     * @param end       the end year
     * @param gcm       the GCM code
     * @param scenario  the emissions scenario
     * @param variable  the Variable code
     */
    public WBClimateKey(Year start, Year end, WBClimate.GCM gcm, WBClimate.SRES scenario, WBClimate.Variable variable) {
        Asserts.notNull(gcm, "The GCM code cannot be null");
        Asserts.notNull(variable, "The Variable code cannot be null");
        Asserts.notNull(start, "The start year cannot be null");
        Asserts.notNull(end, "The end year cannot be null");
        this.start = start;
        this.end = end;
        this.gcm = gcm;
        this.scenario = scenario;
        this.variable = variable;
    }


    /**
     * Returns the start year for this key
     * @return      the start year
     */
    public Year getStart() {
        return start;
    }

    /**
     * Returns the end year for this key
     * @return  the end year
     */
    public Year getEnd() {
        return end;
    }

    /**
     * Returns the GCM code for this key
     * @return      the GCM code
     */
    public WBClimate.GCM getGcm() {
        return gcm;
    }

    /**
     * Returns the emissions scenario for this key
     * @return      the emissions scenario
     */
    public WBClimate.SRES getScenario() {
        return scenario;
    }

    /**
     * Returns the variables for this key
     * @return  the variable for this key
     */
    public WBClimate.Variable getVariable() {
        return variable;
    }


    @Override
    public String toString() {
        return String.format("[%s-%s, %s, %s, %s]", start, end, gcm.getCode(), scenario.getCode(), variable.name());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WBClimateKey)) return false;
        final WBClimateKey that = (WBClimateKey) o;
        if (!getStart().equals(that.getStart())) return false;
        if (!getEnd().equals(that.getEnd())) return false;
        if (getGcm() != that.getGcm()) return false;
        if (getScenario() != that.getScenario()) return false;
        return getVariable() == that.getVariable();
    }

    @Override
    public int hashCode() {
        int result = getStart().hashCode();
        result = 31 * result + getEnd().hashCode();
        result = 31 * result + getGcm().hashCode();
        result = 31 * result + getScenario().hashCode();
        result = 31 * result + getVariable().hashCode();
        return result;
    }
}
