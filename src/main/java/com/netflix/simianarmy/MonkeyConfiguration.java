/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.simianarmy;

/**
 * The Interface MonkeyConfiguration.
 */
public interface MonkeyConfiguration {

    /**
     * Gets the boolean associated with property string. If not found it will return false.
     *
     * @param property
     *            the property
     * @return the bool
     */
    boolean getBool(String property);

    /**
     * Gets the boolean associated with property string. If not found it will return dflt.
     *
     * @param property
     *            the property
     * @param dflt
     *            the dflt
     * @return the bool or else
     */
    boolean getBoolOrElse(String property, boolean dflt);

    /**
     * Gets the number (double) associated with property string. If not found it will return dflt.
     *
     * @param property
     *            the property
     * @param dflt
     *            the dflt
     * @return the num or else
     */
    double getNumOrElse(String property, double dflt);

    /**
     * Gets the string associated with property string. If not found it will return null.
     *
     * @param property
     *            the property
     * @return the str
     */
    String getStr(String property);

    /**
     * Gets the string associated with property string. If not found it will return dflt.
     *
     * @param property
     *            the property
     * @param dflt
     *            the dflt
     * @return the str or else
     */
    String getStrOrElse(String property, String dflt);

    /**
     * If the configuration has dynamic elements then they should be reloaded with this.
     */
    void reload();

    /**
     * Reloads the properties of specific group.
     * @param groupName
     *          the instance group's name
     */
    void reload(String groupName);
}
