package com.baletu.datasync.support;

import com.baletu.datasync.config.LeafConfig;
import com.sankuai.inf.leaf.exception.InitException;
import com.sankuai.inf.leaf.service.SegmentService;

import java.sql.SQLException;

public class DataIdGen {

    private static SegmentService segmentService;

    public static SegmentService getInstance(LeafConfig leafConfig) {
        if(segmentService == null){
            synchronized(DataIdGen.class) {
                if(segmentService == null) {
                    try {
                        segmentService = new SegmentService(leafConfig.getSegment().getUrl(),
                                leafConfig.getSegment().getUsername(), leafConfig.getSegment().getPassword());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (InitException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return segmentService;
    }

}
