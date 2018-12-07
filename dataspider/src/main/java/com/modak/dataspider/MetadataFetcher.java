/*Copyright 2018 modakanalytics.com

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.*/
package com.modak.dataspider;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This abstract MetadataFetcher class defines a metadata fetcher entity and its behavior/methods specifications
 * @author modakanalytics
 * @version 1.0
 */
public abstract class MetadataFetcher implements Runnable {

    public abstract void run();

    public abstract ArrayList<HashMap<String, String>> getMetaData();

    public abstract void init(ConnectAttributes connectAttributes, HashMap<String, String> koshAttributes,
        HashMap<String, String> runAttributes, long crawljobno);
}
