package com.westar.demo;

import no.ecc.vectortile.VectorTileEncoder;

import java.util.HashMap;
/**
 *  使用Map保存每一个level中的所有矢量片编码器
 *      key是这个level中的切片的横纵坐标值(唯一标识一个切片)
 *      value是表示每一个切片对应的矢量片编码器
 */
public class VectorTileEncoderMaps {

    private HashMap<String,VectorTileEncoder> vteMaps = new HashMap<String, VectorTileEncoder>();

    public VectorTileEncoder getVte(String tileName){
        VectorTileEncoder  vte = vteMaps.get(tileName);
        if(null == vte){
            vte = new VectorTileEncoder(4096, 16, false);
            vteMaps.put(tileName, vte);
        }
        return vte;
    }

    public HashMap<String,VectorTileEncoder> getVteMaps(){
        return vteMaps;
    }

}
