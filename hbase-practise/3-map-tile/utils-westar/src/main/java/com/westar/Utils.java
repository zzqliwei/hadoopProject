package com.westar;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    /**
     * 获得指定的区域在指定的级别上被切成哪些切片
     * @param polygon 区域
     * @param level 级别
     * @return 所有的切片
     */
    public static List<String> getTiles(Geometry polygon,int level){
        List<String> tiles = new ArrayList<>();
        double resolution = Tile.resolution/(1 << level);//得到该层级的resolution

        //得到外接矩形的最大最小x,y
        Envelope env = polygon.getEnvelopeInternal();

        //得到开始列对应切片x
        int startColumn = (int)Math.floor((env.getMinX() - Tile.orginX) / resolution / Tile.TILE_SIZE);
        int startRow = (int) Math.floor((Tile.orginY - env.getMaxY()) / resolution / Tile.TILE_SIZE);//得到开始行
        int endColumn = (int) Math.ceil((env.getMaxX() - Tile.orginX) / resolution / Tile.TILE_SIZE);//得到结束列
        int endRow = (int) Math.ceil((Tile.orginY - env.getMinY()) / resolution / Tile.TILE_SIZE);//得到结束行
        if(startColumn == endColumn && startRow == endRow){//如果切成一片
            tiles.add(startColumn + "_" + endRow); //  2_3
            return tiles;
        }else{
            try{
                for(int i=startColumn;i<=endColumn;i++){
                    for(int j=startRow;j<=endRow;j++){
                        Geometry tilePolygon = new WKTReader().read(buildTilePolygonStr(i, j, level)); // 对每一个切片构建一个Polygon
                        if(tilePolygon.intersects(polygon)){ //看看切片的区域是否和指定的区域相交
                            tiles.add(i + "_" + j);//相交的话，则表示指定的区域有部分或者全部在这个切片中
                        }
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
            }
        }

        return tiles;

    }
    /**
     * 将指定的切片在指定的分辨率下转换成Polygon字符串
     *
     * @param x 切片的起始列
     * @param y 切片的起始行
     * @param level 级别
     * @return Polygon字符串
     */
    private static String buildTilePolygonStr(int x, int y, int level) {
        StringBuilder sb = new StringBuilder("POLYGON ((");
        double lngLeft = tileXToX(x, level) + Tile.orginX; //左经度
        double latUp = Tile.orginY - tileYToY(y, level); //上纬度
        double lngRight = tileXToX(x + 1, level) + Tile.orginX; //右经度
        double latDown = Tile.orginY - tileYToY(y + 1, level); //下纬度

        sb.append(lngLeft + " " + latUp + ", ");
        sb.append(lngRight + " " + latUp + ", ");
        sb.append(lngRight + " " + latDown + ", ");
        sb.append(lngLeft + " " + latDown + ", ");
        sb.append(lngLeft + " " + latUp + ")) ");
        return sb.toString();
    }

    private static double tileYToY(int tileY, int level) {
        return pixel2Coordinate(tileY * Tile.TILE_SIZE, level);
    }

    private static double tileXToX(int tileX, int level) {
        return pixel2Coordinate(tileX * Tile.TILE_SIZE, level);
    }

    /**
     *  将像素转成坐标值
     * @param pixel 像素
     * @param level 级别
     * @return
     */
    private static double pixel2Coordinate(double pixel, int level) {
        double resolution = Tile.resolution / (1 << level);
        return pixel * resolution;
    }

    /**
     * 计算一个区域在指定分辨率、指定切片中的位置信息(像素)
     *
     * @param polygon
     * @param startColumn
     * @param startRow
     * @param level
     */
    public static void convert2Pixel(Geometry polygon, int startColumn, int startRow, int level) {
        double resolution = Tile.resolution / (1 << level);
        Coordinate[] cs = polygon.getCoordinates();
        // 修改区域所有坐标点的x为相对于切片起始列的长度大小(像素)
        // 修改区域所有坐标点的y为相对于切片起始行的长度大小(像素)
        for (Coordinate c : cs) {
            c.x = (int) (((c.x - Tile.orginX) / resolution / Tile.TILE_SIZE - startColumn) * 16 * Tile.TILE_SIZE);
            c.y = (int) (((Tile.orginY - c.y) / resolution / Tile.TILE_SIZE - startRow) * 16 * Tile.TILE_SIZE);
        }
    }
}
