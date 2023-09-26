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
@Primary
@MapperScan(basePackages = "com.myself.big.data.core.dao.**", sqlSessionFactoryRef = "mySqlSessionFactory")
public class MysqlDataSourceConfig {

    public static String MYSQL_LOCATION_PATTERN = "classpath:/mapper/.xml";


    @Primary    //配置该数据源为主数据源
    // 保留这个是为了兼容其他模块的
    // spring.datasource.url  spring.datasource.username spring.datasource.password 三个配置
    @Bean(name = "mySqlConnectProperties")
    @ConfigurationProperties(prefix = "spring.datasource")   //application.properties配置文件中该数据源的配置前缀
    public DataSourceProperties mySqlConnectProperties() {
        return new DataSourceProperties();
    }

    @Primary
    @Bean(name ="mySqlDataSourceProperties") // 使用HikariDataSource链接池
    @ConfigurationProperties(prefix = "spring.datasource.hikari")   //application.properties配置文件中该数据源的配置前缀
    public HikariConfig mySqlDataSourceProperties() {
        HikariConfig hikariConfig = new HikariConfig();
        // Hikari连接池配置中加入url username password 配置
        hikariConfig.setJdbcUrl(mySqlConnectProperties().getUrl());
        hikariConfig.setUsername(mySqlConnectProperties().getUsername());
        hikariConfig.setPassword(mySqlConnectProperties().getPassword());
        return hikariConfig;
    }

    @Primary    //配置该数据源为主数据源
    @Bean(name = "mySqlDataSource")
    public DataSource dataSource() {
        // 这里只加载了数据源的 url userName password  但是没有数据库链接池的相关配置
        // HikariDataSource build = mySqlConnectProperties().initializeDataSourceBuilder().type(HikariDataSource.class).build();
        return new HikariDataSource(mySqlDataSourceProperties());
    }

    @Primary
    @Bean(name = "mySqlSessionFactory")
    public SqlSessionFactory MySqlSessionFactory(@Qualifier("mySqlDataSource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(MYSQL_LOCATION_PATTERN));
        // 开启驼峰命名
        sessionFactory.getObject().getConfiguration().setMapUnderscoreToCamelCase(true);
        return sessionFactory.getObject();
    }


    // 开启事务
    @Bean
    @Primary
    public DataSourceTransactionManager mysqlTransactionManager(@Qualifier("mySqlDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }


}
