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

import java.lang.reflect.Type;
import java.time.Year;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.wb.WBException;

/**
 * A class that defines various internal enums relevant to the World Bank Climate API.
 *
 * @see <a href="https://datahelpdesk.worldbank.org/knowledgebase/articles/902061-climate-data-api">World Bank Climate API</a>
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class WBClimate {


    /**
     * An enum that defines the variables capture in various climate models
     */
    public enum Variable {

        TEMPERATURE("tas"),
        PRECIPITATION("pr");

        private static Map<String,Variable> variableMap = new HashMap<>();

        /**
         * Static initializer
         */
        static {
            for (Variable var: Variable.values()) {
                variableMap.put(var.getCode(), var);
            }
        }

        private String code;

        /**
         * Constructor
         * @param code  the world bank code for this variablke
         */
        Variable(String code) {
            this.code = code;
        }

        /**
         * Returns the WB code for this variable
         * @return  the WB code for variable
         */
        public String getCode() {
            return code;
        }
    }


    /**
     * An enum that defines various emissions scenarios used in various climate models
     * @see <a href="http://www.ipcc.ch/ipccreports/sres/emission/index.php?idp=3">Emission Scenarios</a>
     */
    public enum SRES {

        _A2("a2"),
        _B1("b1"),
        _20C3M("20c3m");

        private static Map<String,SRES> sresMap = new HashMap<>();

        /**
         * Static initializer
         */
        static {
            for (SRES sres : SRES.values()) {
                sresMap.put(sres.getCode(), sres);
            }
        }

        private String code;

        /**
         * Constructor
         * @param code  the WB code
         */
        SRES(String code) {
            this.code = code;
        }

        /**
         * The World Bank code for this emissions scenario
         * @return      the World Bank code
         */
        public String getCode() {
            return code;
        }

        /**
         * Returns the SRES for the code specified
         * @param code      the World Bank code
         * @return          the SRES match
         */
        public static SRES getFromCode(String code) {
            return Optional.ofNullable(sresMap.get(code)).orElseThrow(() -> new WBException("No SRES entry matched for code: " + code));
        }

    }


    /**
     * An enum that defines a General Circulation Model
     * @see <a href="http://www.ipcc-data.org/guidelines/pages/gcm_guide.html">General Circulation Models</a>
     */
    public enum GCM {

        BCM_2_0("bccr_bcm2_0", "BCM 2.0", "http://www-pcmdi.llnl.gov/ipcc/model_documentation/BCCR_BCM2.0.htm"),
        CSIRO_MARK_3_5("csiro_mk3_5", "CSIRO Mark 3.5", "http://www.cawcr.gov.au/publications/technicalreports/CTR_021.pdf"),
        ECHAM_4_6("ingv_echam4", "ECHAM 4.6", "http://www.bo.ingv.it/"),
        CGCM_3_1_T47("cccma_cgcm3_1", "CGCM 3.1 (T47)", "http://www.ec.gc.ca/ccmac-cccma/default.asp?lang=En"),
        CNRM_CM3("cnrm_cm3", "CNRM CM3", "http://www.cnrm.meteo.fr/scenario2004/indexenglish.html"),
        GFDL_CM2_0("gfdl_cm2_0", "GFDL CM2.0", "http://data1.gfdl.noaa.gov/nomads/forms/deccen/CM2.X"),
        GFDL_CM2_1("gfdl_cm2_1", "GFDL CM2.1", "http://data1.gfdl.noaa.gov/nomads/forms/deccen/CM2.X"),
        IPSL_CM4("ipsl_cm4", "IPSL-CM4", "http://mc2.ipsl.jussieu.fr/simules.html"),
        MIROC_3_2_MEDRES("miroc3_2_medres", "MIROC 3.2 (medres)", "https://esg.llnl.gov:8443/metadata/browseCatalog.do?uri=http://esgcet.llnl.gov/metadata/pcmdi/ipcc/thredds/miroc3_2_medres.sresb1/pcmdi.ipcc4.miroc3_2_medres.sresb1.thredds"),
        ECHO_G("miub_echo_g", "ECHO-G", "http://www-pcmdi.llnl.gov/projects/modeldoc/cmip/echo-g_tbls.html"),
        ECHAM5_MPI_OM("mpi_echam5", "ECHAM5/MPI-OM", "http://www.mpimet.mpg.de/en/science/models/echam.html"),
        MRI_CGCM2_3_2("mri_cgcm2_3_2a", "MRI-CGCM2.3.2", "http://www.mri-jma.go.jp/Welcome.html"),
        INMCM3_0("inmcm3_0", "INMCM3.0", "http://www.ipcc-data.org/ar4/model-INM-CM3.html"),
        UKMO_HadCM3("ukmo_hadcm3", "UKMO HadCM3", "http://www.metoffice.gov.uk/research/modelling-systems/unified-model/climate-models/hadcm3"),
        UKMO_HadGEM1("ukmo_hadgem1", "UKMO HadGEM1", "http://www.metoffice.gov.uk/research/modelling-systems/unified-model/climate-models/hadgem1"),;


        private static Map<String,GCM> gcmMap = new HashMap<>();

        /**
         * Static initializer
         */
        static {
            for (GCM gcm : GCM.values()) {
                gcmMap.put(gcm.getCode(), gcm);
            }
        }

        private String code;
        private String name;
        private String url;

        /**
         * Constructor
         * @param name  the display name for this model
         * @param url   the url for this model
         */
        GCM(String code, String name, String url) {
            this.code = code;
            this.name = name;
            this.url = url;
        }

        /**
         * Returns the WB code for this variable
         * @return  the WB code
         */
        public String getCode() {
            return code;
        }

        /**
         * Returns the display name for this model
         * @return  the display name for model
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the URL for this model
         * @return  the URL for this model
         */
        public String getUrl() {
            return url;
        }

        /**
         * Returns the GCM for the code specified
         * @param code      the World Bank code
         * @return          the GCM match
         */
        public static GCM getFromCode(String code) {
            return Optional.ofNullable(gcmMap.get(code)).orElseThrow(() -> new WBException("No GCM entry matched for code: " + code));
        }
    }

    /**
     * A record that models monthly average data from World Bank Data API
     */
    public static class MonthlyRecord {

        private WBClimateKey key;
        private double[] values;

        /**
         * Constructor
         * @param values        the montly values, array of length 12
         */
        public MonthlyRecord(WBClimateKey key, double[] values) {
            Asserts.notNull(key, "The key cannot be null");
            Asserts.notNull(values, "The monthly values cannot be null");
            Asserts.assertTrue(values.length == 12, "The monthly values should contains 12 entries");
            this.key = key;
            this.values = values;
        }

        /**
         * Returns the key for this record
         * @return  the key for record
         */
        public WBClimateKey getKey() {
            return key;
        }

        /**
         * Returns the values for this record
         * @return      the values for this record
         */
        public double[] getValues() {
            return values;
        }
    }


    /**
     * A deserializer for WBRegion objects
     */
    public static class MonthlyRecordDeserializer implements JsonDeserializer<MonthlyRecord> {
        @Override
        public MonthlyRecord deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            final JsonObject object = element.getAsJsonObject();
            final SRES scenario = object.has("scenario") ? SRES.getFromCode(object.get("scenario").getAsString()) : SRES._20C3M;
            final GCM gcm = GCM.getFromCode(object.get("gcm").getAsString());
            final Year start = Year.of(object.get("fromYear").getAsInt());
            final Year end = Year.of(object.get("toYear").getAsInt());
            final Variable var = Variable.variableMap.get(object.get("variable").getAsString());
            final JsonArray values = object.get("monthVals").getAsJsonArray();
            final double[] doubles = new double[values.size()];
            for (int i=0; i<doubles.length; ++i) {
                doubles[i] = values.get(i).getAsDouble();
            }
            final WBClimateKey key = new WBClimateKey(start, end, gcm, scenario, var);
            return new MonthlyRecord(key, doubles);
        }
    }



}
