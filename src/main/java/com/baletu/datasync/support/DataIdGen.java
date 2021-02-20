package com.baletu.datasync.support;

import com.baletu.datasync.config.LeafConfig;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.exception.InitException;
import com.sankuai.inf.leaf.service.SegmentService;

import java.sql.SQLException;

public class DataIdGen {

    private static IDGen idGen;

    public static IDGen getInstance(LeafConfig leafConfig) {
        if(idGen == null){
            synchronized(DataIdGen.class) {
                if(idGen == null) {
                    try {
                        idGen = new SegmentService(leafConfig.getSegment().getUrl(),
                                leafConfig.getSegment().getUsername(), leafConfig.getSegment().getPassword()).getIdGen();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (InitException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return idGen;
    }

}
