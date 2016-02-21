package com.redislabs.research;

import com.google.gson.Gson;
import com.redislabs.research.redis.JSONStore;
import com.redislabs.research.redis.PartitionedIndex;
import com.sun.deploy.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dvirsky on 21/02/16.
 */
public class Main {

    public static class GeoJsonEntity {
        public static class Geometry  {
            public float[] coordinates;
            public String type;
        }

        public static class Classifier {
            public String category;
            public String subcategory;
            public String type;
        }

        public static class Properties {
            public String address;
            public String city;
            public Classifier[] classifiers;

            public String country;
            public String href;
            public String name;
            public String owner;
            public String phone;
            public String postcode;
            public String province;
            public String[] tags;
        }

        public Geometry geometry;
        public String id;
        public Properties properties;
        public String type;
    }


    static void loadData(String fileName, Index idx, DocumentStore store) throws IOException {

        Path path = FileSystems.getDefault().getPath("", fileName);
        BufferedReader rd =
                Files.newBufferedReader(path, Charset.forName("utf-8"));

        String line = null;
        Gson gson = new Gson();
        int  i =0;
        while ((line = rd.readLine()) != null) {

            GeoJsonEntity ent = gson.fromJson(line, GeoJsonEntity.class);

            Document doc = new Document(ent.id);
            doc.set("name", ent.properties.name);
            doc.set("city", ent.properties.city);
            doc.set("latlon", ent.geometry.coordinates);

            idx.index(doc);
            store.store(doc);

            i++;
            if (i % 10000 == 0) {
                System.out.println(i);
            }

        }


    }


    public static void main(String [] args) throws IOException {

        Spec spec = new Spec(Spec.prefix("name"));


        PartitionedIndex pi = new PartitionedIndex("locs_name", spec, 8, 500,
                "redis://localhost:6370","redis://localhost:6371", "redis://localhost:6372", "redis://localhost:6373",
                "redis://localhost:6374","redis://localhost:6375", "redis://localhost:6376", "redis://localhost:6377");

        DocumentStore st = new JSONStore("redis://localhost:6379");


        try {

            long stm = System.currentTimeMillis();
            int N = 10000;
            for (int i=0; i < N; i++) {
                List<String> ids = pi.get(new Query("locs_name").filterPrefix("name", "doc"));
            }

            long etm = System.currentTimeMillis();

            //List<Document> docs = st.load(ids.toArray(new String[ids.size()]));
            System.out.println((double)N/((double)(etm-stm)/1000F));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //pi.drop();
        //loadData(args[0], pi, st);
    }
}
