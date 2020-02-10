package com.westar.demo;

import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import com.westar.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExportPbf {

    private final static String path = "D:/pbf/";
    private final static String suffixName = ".pbf";

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        // 读取杭州地图信息并解析需要的id、layerName、height，polygon信息
        Pattern pattern = Pattern.compile("([-.\\d]+)," +
                "([-.\\d]+)," +
                "([^\\\"]+)," +
                "([-.\\d]+)," +
                "([-.\\d]+)," +
                "(POLYGON\\(\\([^\\\"]+\\)\\))," +
                "([-.\\d]+)," +
                "(POLYGON\\(\\([^\\\"]+\\)\\))");

        InputStream inputStream = ExportPbf.class.getClassLoader().getResourceAsStream("hz_building.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        List<Map<String,String>> polygons = new ArrayList<>();
        String line = null;
        while ((line = br.readLine())!=null){
            Matcher matcher = pattern.matcher(line);
            if(matcher.find()){
                Map<String,String> map = new HashMap<>();
                map.put("id", matcher.group(2));
                map.put("name", matcher.group(3));
                map.put("height", matcher.group(7));
                map.put("polygon", matcher.group(6));
                polygons.add(map);
            }
        }

        int startLevel = 0; // 起始等级(大分辨率)
        int endLevel = 0; // 终止等级(小分辨率)
        //每隔level进行串行跑  ---> 每隔level并行跑
        for (int level = startLevel;level <= endLevel;level ++ ){
            VectorTileEncoderMaps tileEncoderMaps = new VectorTileEncoderMaps();
            for(Map<String,String> m:polygons){
                // 计算每一个区域属于哪些片
                String polygonStr = m.get("polygon");
                Geometry polygon = null;
                try{
                    polygon = new WKTReader().read(polygonStr);
                    // 计算出当前区域在这个level上属于哪些片
                    // 每一个切片使用 i_j 来唯一标识，i表示这个切片的开始列，j表示这个切片的开始行
                    List<String> tiles = Utils.getTiles(polygon, level);
                    Map<String,Object> mapClone = new HashMap<String, Object>();
                    mapClone.putAll(m);
                    // 当前的区域转换成每一个片中的像素坐标
                    for(String tile:tiles){
                        System.out.println(tile);
                        Geometry tempPolygon = polygon.copy(); //防止polygon对象的属性值被改变
                        String[] xy = tile.split("_"); // 3_1
                        //相对于切片的起始行和起始列重新计算polygon的所有的点的x和y值
                        Utils.convert2Pixel(tempPolygon, Integer.parseInt(xy[0]), Integer.parseInt(xy[1]), level);
                        mapClone.remove("polygon");

                        // 将改变坐标了的geom放到对应切片的矢量编码器中
                        VectorTileEncoder vte = tileEncoderMaps.getVte(tile);
                        vte.addFeature("hz_building", mapClone, tempPolygon);
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            // 将每一个片的信息写到对应的文件中
            HashMap<String, VectorTileEncoder> vteMaps = tileEncoderMaps.getVteMaps();
            for (String tile : vteMaps.keySet()) {
                VectorTileEncoder vte = vteMaps.get(tile);
                if (vte.encode().length > 0) {
                    try {
                        File pbfFile = new File(path + File.separator + level);
                        if (!pbfFile.exists())
                            pbfFile.mkdirs();//如果文件夹目录不存在，就创建
                        BufferedOutputStream bos = new BufferedOutputStream(
                                new FileOutputStream(path + level + File.separator + tile + suffixName));
                        bos.write(vte.encode());
                        System.out.println("写入文件" + tile + ".pbf");
                        bos.flush();
                        bos.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        }
        System.out.println(System.currentTimeMillis() - start);
    }

}
