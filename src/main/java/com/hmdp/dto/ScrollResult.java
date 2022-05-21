package com.hmdp.dto;

import lombok.Data;

import java.util.List;

/**
 * ScrollResult 滚动分页结果 
 * @author wendong 
 * @version V1.0
 * @date 2022/05/22 02:41
**/
@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime;
    private Integer offset;
}
