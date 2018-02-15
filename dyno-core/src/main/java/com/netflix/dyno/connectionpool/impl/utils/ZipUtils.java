/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.utils;

import org.apache.commons.io.IOUtils;

import com.sun.jersey.core.util.Base64;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class ZipUtils {
    private ZipUtils() {
    }

    public static byte[] compressString(String value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(value.length());
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(Base64.encode(value.getBytes(StandardCharsets.UTF_8)));
        }
        byte[] compressed = baos.toByteArray();
        baos.close();
        return compressed;
    }

    /**
     * Encodes the given string and then GZIP compresses it.
     *
     * @param value String input
     * @return compressed byte array output
     * @throws IOException
     */
    public static byte[] compressStringNonBase64(String value) throws IOException {
        return compressBytesNonBase64(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encodes the given byte array and then GZIP compresses it.
     *
     * @param value byte array input
     * @return compressed byte array output
     * @throws IOException
     */
    public static byte[] compressBytesNonBase64(byte[] value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(value.length);
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(value);
        }
        byte[] compressed = baos.toByteArray();
        baos.close();
        return compressed;
    }

    /**
     * Decompresses the given byte array without transforming it into a String
     *
     * @param compressed byte array input
     * @return decompressed data in a byte array
     * @throws IOException
     */
    public static byte[] decompressBytesNonBase64(byte[] compressed) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(compressed);
        try (InputStream gis = new GZIPInputStream(is)) {
            return IOUtils.toByteArray(gis);
        }
    }

    /**
     * Decompresses the given byte array
     *
     * @param compressed byte array input
     * @return decompressed data in string format
     * @throws IOException
     */
    public static String decompressStringNonBase64(byte[] compressed) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(compressed);
        try (InputStream gis = new GZIPInputStream(is)) {
            return new String(IOUtils.toByteArray(gis), StandardCharsets.UTF_8);
        }
    }

    /**
     * Encodes the given string with Base64 encoding and then GZIP compresses it. Returns
     * result as a Base64 encoded string.
     *
     * @param value input String
     * @return Base64 encoded compressed String
     * @throws IOException
     */
    public static String compressStringToBase64String(String value) throws IOException {
        return new String(Base64.encode(compressString(value)), StandardCharsets.UTF_8);
    }

    /**
     * Decompresses the given byte array and decodes with Base64 decoding
     *
     * @param compressed byte array input
     * @return decompressed data in string format
     * @throws IOException
     */
    public static String decompressString(byte[] compressed) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(compressed);
        try (InputStream gis = new GZIPInputStream(is)) {
            return new String(Base64.decode(IOUtils.toByteArray(gis)), StandardCharsets.UTF_8);
        }
    }

    /**
     * Given a Base64 encoded String, decompresses it.
     *
     * @param compressed Compressed String
     * @return decompressed String
     * @throws IOException
     */
    public static String decompressFromBase64String(String compressed) throws IOException {
        return decompressString(Base64.decode(compressed.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Determines if a byte array is compressed. The java.util.zip GZip
     * implementation does not expose the GZip header so it is difficult to determine
     * if a string is compressed.
     *
     * @param bytes an array of bytes
     * @return true if the array is compressed or false otherwise
     * @throws java.io.IOException if the byte array couldn't be read
     */
    public static boolean isCompressed(byte[] bytes) throws IOException {
        return bytes != null && bytes.length >= 2 &&
                bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC) && bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8);
    }

    /**
     * Determines if an InputStream is compressed. The java.util.zip GZip
     * implementation does not expose the GZip header so it is difficult to determine
     * if a string is compressed.
     *
     * @param inputStream an array of bytes
     * @return true if the stream is compressed or false otherwise
     * @throws java.io.IOException if the byte array couldn't be read
     */
    public static boolean isCompressed(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        byte[] data = new byte[2];

        int nRead = inputStream.read(data, 0, 2);
        buffer.write(data, 0, nRead);
        buffer.flush();

        return isCompressed(buffer.toByteArray());
    }

    /**
     * Determines if a String is compressed. The input String <b>must be Base64 encoded</b>.
     * The java.util.zip GZip implementation does not expose the GZip header so it is difficult to determine
     * if a string is compressed.
     *
     * @param input String
     * @return true if the String is compressed or false otherwise
     * @throws java.io.IOException if the byte array of String couldn't be read
     */
    public static boolean isCompressed(String input) throws IOException {
        return input != null && Base64.isBase64(input) &&
                isCompressed(Base64.decode(input.getBytes(StandardCharsets.UTF_8)));
    }
}