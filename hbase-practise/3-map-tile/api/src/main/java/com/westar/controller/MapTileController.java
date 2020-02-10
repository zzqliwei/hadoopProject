package com.westar.controller;

import com.westar.model.Tile;
import com.westar.pojo.RequestInfo;
import com.westar.service.MapTileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
public class MapTileController {
    private static Logger LOG = LoggerFactory.getLogger(MapTileController.class);

    @Autowired
    private MapTileService mapTileService;

    @ResponseBody
    @RequestMapping(value = "/tiles",method = RequestMethod.POST)
    public List<Tile> requestTilesByTileName(@RequestBody RequestInfo requestInfo){
        LOG.info("request : " + requestInfo);
        //1、接受并解析参数
        int level = requestInfo.getLevel();
        List<String> tileNames = requestInfo.getTiles();

        //2、调用Service服务拿到想要的数据们就是list<Tile>
        List<Tile> tiles = mapTileService.findTilesByTileName(level,tileNames);
        //3、返回给前端
        return tiles;
    }
    @ResponseBody
    @RequestMapping(value = "/tiles/{level}",method = RequestMethod.POST)
    public List<Tile> requestTilesByLevel(@PathVariable int level){
        LOG.info("level : " + level);
        List<Tile> tiles = mapTileService.findTilesByLevel(level);
        //3、返回给前端
        return tiles;
    }
}

