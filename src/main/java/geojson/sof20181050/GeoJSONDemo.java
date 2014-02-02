/*
 *           Copyright 2013 - Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geojson.sof20181050;

import static com.allanbank.mongodb.builder.GeoJson.p;
import static java.util.Arrays.asList;

import com.allanbank.mongodb.builder.GeoJson;

/**
 * GeoJSONDemo provides an introduction to the {@link GeoJson} support class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GeoJSONDemo {
    /**
     * Run the demo.
     * 
     * @param args
     *            Command line arguments. Ignored.
     */
    @SuppressWarnings("unchecked")
    public static void main(final String[] args) {

        System.out.println("Point: " + GeoJson.point(GeoJson.p(1.23, 4.56)));

        System.out.println("Line String: "
                + GeoJson.lineString(p(1.23, 4.56), p(7.89, 10.11)));

        System.out.println("Polygon: "
                + GeoJson.polygon(asList(p(1.23, 4.56), p(7.89, 10.11),
                        p(12.13, 14.15), p(1.23, 4.56))));

        System.out
                .println("MultiLineString: "
                        + GeoJson.multiLineString(
                                asList(p(1.23, 4.56), p(7.89, 10.11),
                                        p(12.13, 14.15)),
                                asList(p(1.23, 4.56), p(7.89, 10.11),
                                        p(12.13, 14.15))));

        System.out.println("MultiLineString: "
                + GeoJson.multiPoint(p(1.23, 4.56), p(7.89, 10.11),
                        p(12.13, 14.15)));

    }
}
