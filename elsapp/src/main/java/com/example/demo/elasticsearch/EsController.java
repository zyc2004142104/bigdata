package com.example.demo.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.httpclient.util.DateUtil;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

@RestController
public class EsController {
    /**
     * 测试索引
     */
    private String indexName="test_index";

    /**
     * 类型
     */
    private String esType="external";

    public  boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
    /**
     * http://127.0.0.1:8080/es/createIndex
     * 创建索引
     * @param request
     * @param response
     * @return
     */
    @RequestMapping("/createIndex")
    @ResponseBody
    public String createIndex(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if(!ElasticsearchUtil.isIndexExist(indexName)) {
            ElasticsearchUtil.createIndex(indexName);
        }
        else{
            return "索引已经存在";
        }
        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> mappings = new HashMap<>();
       // mappings.put("mappings",mappings);
        Map<String,Object> type = new HashMap<>();

        //source.put( "enabled",false );  //如果有此属性，则无法查找到结果
        Map<String,Object> source=new HashMap<>(  );
        source.put( "enabled",false );
        //List<String> incList = new ArrayList();
        //incList.add( "_id" );
        //source.put("includes",incList);
        type.put("_source", source);
        mappings.put(esType, type);


        Map<String,Object> properties = new HashMap<>();
        type.put("properties", properties);

        // 文档的id映射
        Map<String,Object> idProperties = new HashMap<>();
        idProperties.put("type", "text");
        properties.put("id", idProperties);

        // 文档的age映射
        Map<String,Object> ageProperties = new HashMap<>();
        ageProperties.put("type", "integer");
        properties.put("age", ageProperties);

        // 文档的name映射
        Map<String,Object> nameProperties = new HashMap<>();
        nameProperties.put("type", "keyword");
        properties.put("name", nameProperties);

        // 文档的date映射
        Map<String,Object> dateProperties = new HashMap<>();
        dateProperties.put("type", "date");
        dateProperties.put("format","strict_date_optional_time||epoch_second");
        properties.put("date", dateProperties);
        String json = mapper.writeValueAsString(mappings);
        System.out.println(json);
        ElasticsearchUtil.createMapping( indexName,esType,json );
        return "索引创建成功";
    }

    /**
     * 插入记录
     * @return
     */
    @RequestMapping("/insertJson")
    @ResponseBody
    public String insertJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", DateUtil.formatDate(new Date()));
        jsonObject.put("age", 25);
        jsonObject.put("name", "j-" + new Random(100).nextInt());
        jsonObject.put("date", new Date());
        String id = ElasticsearchUtil.addData(jsonObject, indexName, esType, jsonObject.getString("id"));
        return id;
    }

    /**
     * 插入记录
     * @return
     */
    @RequestMapping("/insertModel")
    @ResponseBody
    public String insertModel() {
        EsModel esModel = new EsModel();
        esModel.setId(DateUtil.formatDate(new Date()));
        esModel.setName("m-" + new Random(100).nextInt());
        esModel.setAge(30);
        esModel.setDate(new Date());
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(esModel);
        System.out.println(jsonObject.toJSONString());
        String id = ElasticsearchUtil.addData(jsonObject, indexName, esType, jsonObject.getString("id"));
        return id;
    }

    /**
     * 删除记录
     * @return
     */
    @RequestMapping("/delete")
    @ResponseBody
    public String delete(String id) {
        if(!isBlank(id)) {
            ElasticsearchUtil.deleteDataById(indexName, esType, id);
            return "删除id=" + id;
        }
        else{
            return "id为空";
        }
    }

    /**
     * 更新数据
     * @return
     */
    @RequestMapping("/update")
    @ResponseBody
    public String update(String id) {
        if(!isBlank(id)) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", id);
            jsonObject.put("age", 31);
            jsonObject.put("name", "修改");
            jsonObject.put("date", new Date());
            ElasticsearchUtil.updateDataById(jsonObject, indexName, esType, id);
            return "id=" + id;
        }
        else{
            return "id为空";
        }
    }

    /**
     * 获取数据
     * http://127.0.0.1:8080/es/getData?id=2018-04-25%2016:33:44
     * @param id
     * @return
     */
    @RequestMapping("/getData")
    @ResponseBody
    public String getData(String id){
        if(!isBlank(id)) {
            Map<String, Object> map= ElasticsearchUtil.searchDataById(indexName,esType,id,null);
            return JSONObject.toJSONString(map);
        }
        else{
            return "id为空";
        }
    }

    /**
     * 查询数据
     * 模糊查询
     * @return
     */
    @RequestMapping("/queryMatchData")
    @ResponseBody
    public String queryMatchData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean matchPhrase = false;
        if (matchPhrase == Boolean.TRUE) {
            boolQuery.must(QueryBuilders.matchPhraseQuery("name", "修"));
        } else {
            boolQuery.must(QueryBuilders.matchQuery("name", "修"));
        }
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null,null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 通配符查询数据
     * 通配符查询 ?用来匹配1个任意字符，*用来匹配零个或者多个字符
     * @return
     */
    @RequestMapping("/queryWildcardData")
    @ResponseBody
    public String queryWildcardData() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "j-*466");
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null,null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 正则查询
     * @return
     */
    @RequestMapping("/queryRegexpData")
    @ResponseBody
    public String queryRegexpData() {
        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("name", "j--[0-9]{1,11}");
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null,null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询数字范围数据
     * @return
     */
    @RequestMapping("/queryIntRangeData")
    @ResponseBody
    public String queryIntRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").from(21)
                .to(25));
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null,null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询日期范围数据
     * @return
     */
    @RequestMapping("/queryDateRangeData")
    @ResponseBody
    public String queryDateRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-10-19T12:27:46.840Z")
                .to("2018-10-20T10:03:08.081Z"));
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, boolQuery, 10, null, "date", "asc",null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询分页
     * @param startPage   第几条记录开始
     *                    从0开始
     *                    第1页 ：http://127.0.0.1:8080/es/queryPage?startPage=0&pageSize=2
     *                    第2页 ：http://127.0.0.1:8080/es/queryPage?startPage=2&pageSize=2
     * @param pageSize    每页大小
     * @return
     */
    @RequestMapping("/queryPage")
    @ResponseBody
    public String queryPage(String startPage,String pageSize){
        if(!isBlank(startPage)&&!isBlank(pageSize)) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-04-25T08:33:44.840Z")
                    .to("2018-04-25T10:03:08.081Z"));
            EsPage list = ElasticsearchUtil.searchDataPage(indexName, esType, Integer.parseInt(startPage), Integer.parseInt(pageSize), boolQuery, null, null, null);
            return JSONObject.toJSONString(list);
        }
        else{
            return  "startPage或者pageSize缺失";
        }
    }
}
