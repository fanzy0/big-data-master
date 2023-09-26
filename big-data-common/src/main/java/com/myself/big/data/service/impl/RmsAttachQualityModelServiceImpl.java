package com.myself.big.data.service.impl;

import com.myself.big.data.core.bean.RmsAttachQualityModelHbase;
import com.myself.big.data.core.dao.RmsAttachQualityModelHbaseMapper;
import com.myself.big.data.service.RmsAttachQualityModelService;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
@Slf4j
public class RmsAttachQualityModelServiceImpl implements RmsAttachQualityModelService {
    @Autowired
    RmsAttachQualityModelHbaseMapper rmsAttachQualityModelHbaseMapper;

    @Autowired
    @Qualifier("phoenixSqlSessionFactory")
    SqlSessionFactory phoenixSqlSessionFactory;


    @Override
    public void createRmsAttachQualityTable(int salt,String dayStr) {
        rmsAttachQualityModelHbaseMapper.createRmsAttachQualityTable(salt, dayStr);
    }

    @Override
    public void appointRmsAttachQualityLocalIndex(String index, int salt,String dayStr) {
        rmsAttachQualityModelHbaseMapper.appointRmsAttachQualityLocalIndex(index, salt, dayStr);
    }

    @Override
    public void appointRmsAttachQualityGlobalIndex(String index,int salt, String dayStr) {
        rmsAttachQualityModelHbaseMapper.appointRmsAttachQualityGlobalIndex(index, salt, dayStr);
    }

    @Override
    public boolean batchInsertRmsAttachQualityTable(List<RmsAttachQualityModelHbase> qualityModelList, String dayStr) {
        SqlSession session = null;
        try {
            long startTime = System.currentTimeMillis();
            //log.warn("batchInsertRmsAttachQualityTable start ,  day [{}],Thread[{}] ", dayStr,Thread.currentThread().getName());
            session = phoenixSqlSessionFactory.openSession(ExecutorType.BATCH, false);
            RmsAttachQualityModelHbaseMapper mapper = session.getMapper(RmsAttachQualityModelHbaseMapper.class);
            for (int i = 0; i < qualityModelList.size(); i++) {
                mapper.upsertIntoRmsAttachQualityTable(qualityModelList.get(i), dayStr);
            }
            session.flushStatements();
            session.commit(true);
            session.close();
            //log.warn("batchInsertRmsAttachQualityTable success , day [{}] , Thread[{}], 耗时 [{}]", dayStr,Thread.currentThread().getName(),(System.currentTimeMillis() - startTime)/1000);
            return true;
        } catch (Exception e) {
            log.error("batchInsertRmsAttachQualityTable error , day [{}], Thread[{}] ", dayStr,Thread.currentThread().getName(), e);
            return false;
        } finally {
            if(session != null){
                session.close();
            }
        }

    }

    @Override
    public List<RmsAttachQualityModelHbase> queryRmsAttachQualityModelsByRmsMac(String rmsMac, String dayStr) {
        return rmsAttachQualityModelHbaseMapper.queryRmsAttachQualityModelsByRmsMac(rmsMac, dayStr);
    }

}
