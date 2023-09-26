package com.myself.big.data.dataSourceConfig;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(basePackages = "com.ai.ints.analytics.core.dao.**", sqlSessionFactoryRef = "phoenixSqlSessionFactory")
public class PhoenixDataSourceConfig {

    public static String PHOENIX_LOCATION_PATTERN = "classpath:/mapper/*.xml";

    @Bean(name = "phoenixDataConnectProperties")
    @ConfigurationProperties(prefix = "spring.datasource.phoenix")   //application.properties配置文件中该数据源的配置前缀
    public DataSourceProperties phoenixDataConnectProperties() {
        return new DataSourceProperties();
    }

//    @Bean(name="dataSourceP")
//    public Properties testPhoenix(){
//        Properties prop = new Properties();
//        prop.put("phoenix.schema.isNamespaceMappingEnabled",true);
//        return prop;
//    }

    @Primary
    @Bean(name ="phoenixDataSourceProperties") // 使用HikariDataSource链接池
    @ConfigurationProperties(prefix = "spring.datasource.phoenix.hikari")   //application.properties配置文件中该数据源的配置前缀
    public HikariConfig phoenixDataSourceProperties() {
        HikariConfig hikariConfig = new HikariConfig();
        // Hikari连接池配置中加入url username password 配置
        hikariConfig.setJdbcUrl(phoenixDataConnectProperties().getUrl());
        hikariConfig.setUsername(phoenixDataConnectProperties().getUsername());
        hikariConfig.setPassword(phoenixDataConnectProperties().getPassword());
//        hikariConfig.setDataSourceProperties(testPhoenix());
        return hikariConfig;
    }


    @Bean(name = "phoenixDataSource")
    public DataSource phoenixDataSource() {
        // return phoenixDataConnectProperties().initializeDataSourceBuilder().type(HikariDataSource.class).build();
        return new HikariDataSource(phoenixDataSourceProperties());
    }

    @Bean(name = "phoenixSqlSessionFactory")
    public SqlSessionFactory phoenixSqlSessionFactory(@Qualifier("phoenixDataSource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(PHOENIX_LOCATION_PATTERN));
        // 开启驼峰命名
        sessionFactory.getObject().getConfiguration().setMapUnderscoreToCamelCase(true);
        return sessionFactory.getObject();
    }

    // 开启事务
    @Bean
    public DataSourceTransactionManager phoenixTransactionManager(@Qualifier("phoenixDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }


}
