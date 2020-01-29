package com.westar.hbase.spark;

import java.util.Random;

/**
 * java -cp practice-1.0-SNAPSHOT-jar-with-dependencies.jar com.twq.hbase.spark.DataGenerator 100000 > ~/hbase-course/spark/data.txt
 * hadoop fs -mkdir -p /user/hadoop-twq/hbase-course/spark
 * hadoop fs -put ~/hbase-course/spark/data.txt /user/hadoop-twq/hbase-course/spark
 *
 * 数据格式：
 *   1933623542a7-5bdb-d564-3133-276ae3ce|lastname|Berrouard
 1933623542a7-5bdb-d564-3133-276ae3ce|city|Montreal
 1933623542a7-5bdb-d564-3133-276ae3ce|postalcode|H2M1N6
 1933623542a7-5bdb-d564-3133-276ae3ce|birthdate|20/03/2000
 1933623542a7-5bdb-d564-3133-276ae3ce|firstname|Maurice
 5658563542a7-5bdb-d564-3133-276ae3ce|status|veuve
 5658563542a7-5bdb-d564-3133-276ae3ce|lastname|Garbuet
 5658563542a7-5bdb-d564-3133-276ae3ce|birthdate|20/05/1946
 5658563542a7-5bdb-d564-3133-276ae3ce|sexe|male
 5658563542a7-5bdb-d564-3133-276ae3ce|city|Miami
 3067653542a7-5bdb-d564-3133-276ae3ce|birthdate|30/06/1998
 9793143542a7-5bdb-d564-3133-276ae3ce|firstname|Audrey
 4822133542a7-5bdb-d564-3133-276ae3ce|lastname|Johns
 4989193542a7-5bdb-d564-3133-276ae3ce|city|Miami
 */
public class DataGenerator {

    protected static final Random random = new Random(System.currentTimeMillis());

    protected static final String[] fields = {"firstname", "lastname", "birthdate", "postalcode", "city", "sexe", "status"};
    protected static final String[] firstNames = {"Maurice", "Kevin", "Groseille", "Gauthier", "Gaelle", "Audrey", "Nicole"};
    protected static final String[] lastNames = {"Spaggiari", "O'Dell", "Berrouard", "Smith", "Johns", "Samuel", "Garbuet"};
    protected static final String[] birthDates = {"06/03/1942", "23/05/1965", "01/11/1977", "30/06/1998", "20/03/2000", "23/09/2002", "20/05/1946"};
    protected static final String[] postalCodes = {"34920", "05274", "H2M1N6", "H2M2V5", "34270", "H0H0H0", "91610"};
    protected static final String[] cities = {"Le Cres", "Miami", "Montreal", "Ottawa", "Maugio", "Cocagne", "Ballancourt"};
    protected static final String[] sexes = {"male", "male", "female", "male", "unknown", "female", "female"};
    protected static final String[] statuses = {"maried", "maried", "en couple", "en couple", "single", "en couple", "veuve"};

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Syntax: DataGenerator iterations_count");
            return;
        }
        long iterations = Long.parseLong(args[0]);
        for(int i=0;i<iterations;i++){
            String fieldName = generateFieldName();
            String uuid = generateUUID();
            System.out.println(new StringBuilder(uuid).reverse().toString() + "|" + fieldName + "|" + generateData(fieldName, uuid));
        }
    }

    private static String generateData(String fieldName, String uuid) {
        if (fieldName.equals("status")) {
            return statuses[Math.abs(uuid.hashCode()) % statuses.length];
        }
        if (fieldName.equals("sexe")) {
            return sexes[Math.abs(uuid.hashCode()) % sexes.length];
        }
        if (fieldName.equals("city")) {
            return cities[Math.abs(uuid.hashCode()) % cities.length];
        }
        if (fieldName.equals("postalcode")) {
            return postalCodes[Math.abs(uuid.hashCode()) % postalCodes.length];
        }
        if (fieldName.equals("birthdate")) {
            return birthDates[Math.abs(uuid.hashCode()) % birthDates.length];
        }
        if (fieldName.equals("lastname")) {
            return lastNames[Math.abs(uuid.hashCode()) % lastNames.length];
        }
        if (fieldName.equals("firstname")) {
            return firstNames[Math.abs(uuid.hashCode()) % firstNames.length];
        }
        return "unknow field type";
    }

    private static String generateFieldName() {
        return fields[random.nextInt(fields.length)];
    }

    /**
     * Generate an almost random UUID (data set one million).
     *
     * @return
     */
    public static String generateUUID() {
        StringBuffer value = new StringBuffer("" + random.nextInt(1000000));
        while (value.length() < 6){
            value.insert(0, "0");

        }
        return "ec3ea672-3313-465d-bdb5-7a2453" + value.toString();
    }
}
