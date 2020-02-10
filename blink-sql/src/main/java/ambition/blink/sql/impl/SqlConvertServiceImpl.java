/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ambition.blink.sql.impl;

import ambition.blink.sql.SqlConstant;
import ambition.blink.sql.SqlConvertService;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ambition.blink.common.exception.FlinkSqlException;
import ambition.blink.common.utils.BlinkStringUtils;

public class SqlConvertServiceImpl implements SqlConvertService {

  private static final Logger LOG = LoggerFactory.getLogger(SqlConvertServiceImpl.class);

  @Override
  public Map<String, List<String>> sqlConvert(String sqlContext) throws Exception {
    if (StringUtils.isBlank(sqlContext)){
      LOG.warn("sql is null");
      throw new FlinkSqlException("input sql is null, sql not null");
    }

    List<String> list = BlinkStringUtils.splitSemiColon(sqlContext);
    if (CollectionUtils.isEmpty(list)) {
      LOG.warn("sql not use quota error");
      throw new FlinkSqlException("sql not use quota error, sql not null");
    }

    if (BlinkStringUtils.isChinese(sqlContext)){
      LOG.info("sql contain Chinese char {} ", sqlContext);
      throw new FlinkSqlException("sql contain Chinese char");
    }

    //sql拆解
    List<String> sqls = BlinkStringUtils.splitSemiColon(sqlContext);
    Map<String,List<String>> result = new HashMap<>();

    //sql顺序的一致
    List<String> funList = new LinkedList<>();
    List<String> tableList = new LinkedList<>();
    List<String> viewList = new LinkedList<>();
    List<String> insertList = new LinkedList<>();

    for (String sql: sqls) {
      if (StringUtils.isNotBlank(sql)){
        if (BlinkStringUtils.isContain(SqlConstant.CREATE_FUNCTION, sql)) {
          funList.add(sql);
        }

        if (BlinkStringUtils.isContain(SqlConstant.CREATE_TABLE, sql)) {
          tableList.add(sql);
        }

        if (BlinkStringUtils.isContain(SqlConstant.CREATE_VIEW, sql)) {
          viewList.add(sql);
        }

        if (BlinkStringUtils.isContain(SqlConstant.INSERT_INTO, sql)) {
          insertList.add(sql);
        }
      }
    }

    if (CollectionUtils.isEmpty(tableList)) {
      LOG.warn("sql not contain create table");
      throw new FlinkSqlException("sql not contain create table");
    }
    result.put(SqlConstant.TABLE, tableList);

    if (CollectionUtils.isEmpty(insertList)) {
      LOG.warn("sql not contain insert into");
      throw new FlinkSqlException("sql not contain insert into");
    }
    result.put(SqlConstant.INSERT, insertList);

    if (CollectionUtils.isNotEmpty(funList)){
      result.put(SqlConstant.FUNCTION, funList);
    }

    if (CollectionUtils.isNotEmpty(viewList)){
      result.put(SqlConstant.VIEW, viewList);
    }

    return result;
  }

}
