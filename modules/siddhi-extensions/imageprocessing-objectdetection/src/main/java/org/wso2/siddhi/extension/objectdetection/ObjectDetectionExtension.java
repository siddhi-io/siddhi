/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.extension.objectdetection;

import nu.pattern.OpenCV;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/**
 * The following class represents a functional siddhi extension. The extension allows to detect
 * objects of a given image using OpenCV functions. The object detection is done using a method
 * called Haar Classifier in image processing context. An xml file is used to describe the type of
 * object needs to be detected. This xml file is also known as a cascade file.
 * </p>
 * The extension take 2 input arguments.
 * 1. The image as a hex string.
 * 2. The cascade file path for the object detection.
 */
@SuppressWarnings("UnusedDeclaration")
@SiddhiExtension(namespace = "imageprocessorobjectdetection", function = "count")
public class ObjectDetectionExtension extends FunctionExecutor {

    /**
     * The logger to log information, warnings or errors.
     */
    Logger log = Logger.getLogger(ObjectDetectionExtension.class);

    /**
     * The return type for the functional extension.
     */
    Attribute.Type returnType;

    /**
     * Loads the native libraries for OpenCV.
     * @see <a href="https://github.com/PatternConsulting/opencv#api">OpenCV API loading.</a>
     */
    static {
        OpenCV.loadLibrary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getReturnType() {
        return returnType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(Type[] types, SiddhiContext arg1) {
        returnType = Attribute.Type.LONG;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Object process(Object obj) {
        long detectedObjectCount = 0;
        if (obj instanceof Object[]) {
            Object[] arguments = (Object[]) obj;
            if (arguments.length == 2) {
                if (arguments[0] instanceof String && arguments[1] instanceof String) {
                    String imageHex = (String) arguments[0];
                    String cascadePath = (String) arguments[1];
                    detectedObjectCount = this.detectObjects(imageHex, cascadePath);
                } else {
                    throw new IllegalArgumentException(
                            "2 String arguments of the hex string of the image and the cascade " +
                            "path is expected.");
                }
            } else {
                throw new IllegalArgumentException(
                        "2 String arguments of the hex string of the image and the cascade path " +
                        "is expected.");
            }
        } else {
            throw new IllegalArgumentException(
                    "2 String arguments of the hex string of the image and the cascade path is " +
                    "expected.");
        }

        return detectedObjectCount;
    }

    /**
     * Uses a given image and a cascade file to detect objects of that image. The object detection
     * occurs in the following process.
     * 1. The hex string of the image is converted to a byte array and then to a
     * {@link org.opencv.core.Mat} object as there is no way of converting the hex string directly
     * to {@link org.opencv.core.Mat} object.
     * 2. {@link org.opencv.objdetect.CascadeClassifier} is loaded up with the given cascade file.
     * 3. The image given is then grayscaled(converted to black and white).
     * 4. The image is then subjected to histogram equalization.
     * 5. Use the classifier to detect the objects.
     * </p>
     * Grayscaling and histogram equalization of the image helps to identify objects better.
     *
     * @param imageHex    The image as a hex string.
     * @param cascadePath The physical path for the cascade file.
     * @return The detected object count
     */
    private long detectObjects(String imageHex, String cascadePath) {
        long objectCount;
        try {
            // conversion to Mat
            byte[] imageByteArr = (byte[]) new Hex().decode(imageHex);
            Mat image = Highgui.imdecode(new MatOfByte(imageByteArr), Highgui.IMREAD_UNCHANGED);

            // initializing classifier
            CascadeClassifier cClassifier = new CascadeClassifier();
            cClassifier.load(cascadePath);

            // pre-processing
            Imgproc.cvtColor(image, image, Imgproc.COLOR_RGB2GRAY);
            Imgproc.equalizeHist(image, image);

            // detecting objects
            MatOfRect imageRect = new MatOfRect();
            cClassifier.detectMultiScale(image, imageRect);

            // image count
            objectCount = ((Integer) imageRect.toList().size()).longValue();
        } catch (DecoderException e) {
            log.error("Unable to decode the hex string of the image.", e);
            throw new RuntimeException("Unable to decode the hex string of the image.", e);
        }

        return objectCount;
    }
}
