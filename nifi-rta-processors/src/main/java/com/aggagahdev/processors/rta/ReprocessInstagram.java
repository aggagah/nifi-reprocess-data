/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aggagahdev.processors.rta;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"custom processor, reprocess data, rta, instagram"})
@CapabilityDescription("Custom NiFi processor built with Java Maven for reprocessing instagram profile data")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ReprocessInstagram extends AbstractProcessor {

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor
            .Builder().name("JOLT_SPEC")
            .displayName("JOLT Specification")
            .description("New JSON Mapping for the FlowFile json using JOLT Specification")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When FlowFile successfully processed, it goes here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When there was error while processing FlowFile, it goes here")
            .build();
    private static final Logger log = LoggerFactory.getLogger(ReprocessInstagram.class);

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(JOLT_SPEC);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            if (context.getProperty(JOLT_SPEC).getValue() == null || context.getProperty(JOLT_SPEC).getValue().isEmpty() || context.getProperty(JOLT_SPEC).getValue().isBlank()){
                // case tanpa JOLT di property
                FlowFile processedFlowfile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                        Map<String, Object> jsonMap = JsonUtils.jsonToMap(inputStream);
                        jsonMap = (Map<String, Object>) jsonMap.get("node");
                        log.info("JSON MAP : "+jsonMap);

                        Map<String, Object> finalMap = new HashMap<>();

                        if(jsonMap instanceof Map){
                            finalMap.put("id", jsonMap.get("pk").toString());
                            finalMap.put("code", jsonMap.get("code").toString());
                            Map<String, Object> captionNode = (Map<String, Object>) jsonMap.get("caption");

                            finalMap.put("caption", captionNode.get("text").toString());

                            long createdNode = ((Integer) jsonMap.get("taken_at")).longValue() * 1000;
                            finalMap.put("created", createdNode);

                            Map<String, Object> globalMap = new HashMap<>();
                            globalMap.put("id", jsonMap.get("id").toString());

                            finalMap.put("global", globalMap);

                            Map<String, Object> ownerMap = (Map<String, Object>) jsonMap.get("owner");
                            Map<String, Object> ownerChild = new HashMap<>();
                            ownerChild.put("id", ownerMap.get("pk"));
                            ownerChild.put("picture", ownerMap.get("profile_pic_url"));
                            ownerChild.put("username", ownerMap.get("username"));

                            finalMap.put("image", ownerMap.get("profile_pic_url"));
                            finalMap.put("owner", ownerChild);

                            Map<String, Object> insightMap = new HashMap<>();
                            insightMap.put("comments", jsonMap.get("comment_count"));
                            insightMap.put("likes", jsonMap.get("like_count"));

                            finalMap.put("insight", insightMap);

                            long timestampNode = ((Integer) captionNode.get("created_at")).longValue() * 1000;
                            finalMap.put("timestamp", timestampNode);

                            // work with carousel
                            ArrayList<Map<String, Object>> finalCarousel = new ArrayList<>();
                            // node carousel
                            ArrayList<Object> carouselOuter = (ArrayList<Object>) jsonMap.get("carousel_media");
                            carouselOuter.forEach(elem -> {
                                Map<String, Object> innerFinal = new HashMap<>();
                                Map<String, Object> carouselInner = (Map<String, Object>) elem;
                                innerFinal.put("id", carouselInner.get("pk"));
                                innerFinal.put("caption", carouselInner.get("accessibility_caption"));
                                long createdAtInner = ((Integer) carouselInner.get("taken_at")).longValue() * 1000;
                                innerFinal.put("created", createdAtInner);

                                Map<String, Object> innerGlobal = new HashMap<>();
                                innerGlobal.put("id", carouselInner.get("id").toString());
                                innerFinal.put("global", innerGlobal);

                                Map<String, Object> innerParent = new HashMap<>();
                                innerParent.put("id", carouselInner.get("carousel_parent_id"));
                                innerFinal.put("parent", innerParent);

                                // process image_versions
                                Map<String, Object> imageVersions = (Map<String, Object>) carouselInner.get("image_versions2");
                                ArrayList<Object> candidates = (ArrayList<Object>) imageVersions.get("candidates");
                                Map<String, Object> innerCandidate = (Map<String, Object>) candidates.get(0);

                                innerFinal.put("image", innerCandidate.get("url").toString());

                                finalCarousel.add(innerFinal);
                            });

                            finalMap.put("carousel", finalCarousel);

                        }

                        String finalJsonString = JsonUtils.toJsonString(finalMap);
                        outputStream.write(finalJsonString.getBytes(StandardCharsets.UTF_8));
                    }
                });

                session.transfer(processedFlowfile, REL_SUCCESS);

            } else{
                // case dengan JOLT di property
                final String joltSpec = context.getProperty(JOLT_SPEC).getValue();

                List<Object> specJson = JsonUtils.jsonToList(joltSpec);

                Chainr chainr = Chainr.fromSpec(specJson);

                FlowFile transformedFlowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                        Map<String, Object> inputJson = JsonUtils.jsonToMap(inputStream);

                        Object transformedJson = chainr.transform(inputJson);

                        Map<String, Object> newMap = (Map<String, Object>) transformedJson;

                        if (newMap.containsKey("created")) {
                            Object createdValue = newMap.get("created");
                            if (createdValue instanceof Integer) {
                                newMap.put("created", ((Integer) createdValue).longValue() * 1000);
                            }
                        }

                        if (newMap.containsKey("timestamp")) {
                            Object timestampValue = newMap.get("timestamp");
                            if (timestampValue instanceof Integer) {
                                newMap.put("timestamp", ((Integer) timestampValue).longValue() * 1000);
                            }
                        }

                        if (newMap.containsKey("carousel")){
                            Object carouselValue = newMap.get("carousel");

                            if(carouselValue instanceof ArrayList){
                                ((ArrayList<?>) carouselValue).forEach(elem -> {
                                    if(elem instanceof Map){
                                        if(((Map) elem).containsKey("created")){
                                            Object createdInsideValue = ((Map) elem).get("created");
                                            if(createdInsideValue instanceof Integer){
                                                ((Map) elem).put("created", ((Integer) createdInsideValue).longValue() * 1000);

                                                log.info("CASTING int to long on Created Inside map succeed");
                                            }

                                        }

                                        if(((Map) elem).containsKey("media")){
                                            Object media = ((Map) elem).get("media");
                                            if(media instanceof Map){
                                                if(((Map<?, ?>) media).containsKey("candidates")){
                                                    Object candidates = ((Map<?, ?>) media).get("candidates");

                                                    if(candidates instanceof ArrayList){
                                                        Object cand = ((ArrayList<?>) candidates).get(0);
                                                        String imageUrl = "";

                                                        ArrayList<String> urls = new ArrayList();
                                                        if (cand instanceof Map){
                                                            String dataInside = (String) ((Map) cand).get("url");
                                                            imageUrl = dataInside;
                                                        }

                                                        ((Map) elem).put("image", imageUrl);
                                                        log.info("CHECK VALUE image : "+((Map) elem).get("image"));
                                                    }
                                                }
                                            }
                                        }

                                        ((Map) elem).remove("media");
                                    }
                                });
                            }
                        }


                        String finalJsonString = JsonUtils.toJsonString(newMap);
                        outputStream.write(finalJsonString.getBytes(StandardCharsets.UTF_8));
                    }
                });

                session.transfer(transformedFlowFile, REL_SUCCESS);
            }

        } catch (Exception e){
            log.error("ERROR : "+e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
