package org.custom;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.io.PathUtils;
import org.wltea.analyzer.dic.Dictionary;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: Zhang Chaoqing
 * Date: 2023/2/6 14:36
 */
public class ExtDictLoader implements Runnable {

    private static final Logger LOGGER = ESPluginLoggerFactory.getLogger(ExtDictLoader.class.getName());

    private static final ExtDictLoader INSTANCE = new ExtDictLoader();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static String URL;

    private static String USERNAME;

    private static String PASSWORD;

    private final AtomicBoolean extWordFirstLoad = new AtomicBoolean(false);

    private final AtomicReference<String> extWordLastLoadTimeRef = new AtomicReference<>(null);

    private final AtomicBoolean stopWordFirstLoad = new AtomicBoolean(false);

    private final AtomicReference<String> stopWordLasLoadTimeRef = new AtomicReference<>(null);


    public static void main(String[] args) {
        ExtDictLoader extDictLoader = new ExtDictLoader();
        extDictLoader.loadExtWords();
    }

    static {

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction) () -> {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            Properties mysqlConfig = new Properties();
            Path configPath = PathUtils.get(Dictionary.getSingleton().getDictRoot(), "jdbc.properties");
            try {
                mysqlConfig.load(new FileInputStream(configPath.toFile()));
//                mysqlConfig.load(new FileInputStream("E:\\code\\elasticsearch-analysis-ik\\config\\jdbc.properties"));
                URL = mysqlConfig.getProperty("jdbc.url");
                USERNAME = mysqlConfig.getProperty("jdbc.username");
                PASSWORD = mysqlConfig.getProperty("jdbc.password");
            } catch (IOException e) {
                throw new IllegalStateException("加载jdbc.properties配置⽂件发⽣异常");
            }
            return null;
        });


    }


    public static ExtDictLoader getInstance() {
        return INSTANCE;
    }


    public void loadExtWords() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String sql;

        if (extWordFirstLoad.compareAndSet(false, true)) {
            // 首次加载全量的词
            sql = "SELECT word FROM t_word";
        } else {
            // 后面按时间加载增量的词
            sql = "SELECT word FROM t_word WHERE update_at >='" + extWordLastLoadTimeRef.get() + "'";
        }

        extWordLastLoadTimeRef.set(DATE_TIME_FORMATTER.format(LocalDateTime.now()));
        // 加载扩展词词库内容
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            LOGGER.info("从mysql加载extWord, sql={}", sql);
            Set<String> extensionWords = new HashSet<>();
            while (resultSet.next()) {
                String word = resultSet.getString("word");
                if (word != null) {
                    extensionWords.add(word);
                    LOGGER.info("从mysql加载extWord，word={}", word);
                }
            }
            // 放到字典⾥
            Dictionary.getSingleton().addWords(extensionWords);
        } catch (Exception e) {
            LOGGER.error("从mysql加载extWord发⽣异常", e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
            }
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
            }
        }
    }

    @Override
    public void run() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction) () -> {
            loadExtWords();
            return null;
        });

    }


//    public void loadMysqlStopWords() {
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet resultSet = null;
//        String sql;
//
//        if (stopWordFirstLoad.compareAndSet(false, true)) {
//            sql = "SELECT word FROM stop_word";
//        } else {
//            sql = "SELECT word FROM stop_word WHERE created_time >= '" + stopWordLasLoadTimeRef.get() + "'";
//        }
//        stopWordLasLoadTimeRef.set(DATE_TIME_FORMATTER.format(LocalDateTime.now()));
//        // 加载词库内容
//        try {
//            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
//            statement = connection.createStatement();
//            resultSet = statement.executeQuery(sql);
//            LOGGER.info("从mysql加载stopWord, sql={}", sql);
//            Set<String> stopWords = new HashSet<>();
//            while (resultSet.next()) {
//                String word = resultSet.getString("word");
//                if (word != null) {
//                    stopWords.add(word);
//                    LOGGER.info("从mysql加载stopWord，word={}", word);
//                }
//            }
//            // 放到字典⾥
//            Dictionary.getSingleton().addStopWords(stopWords);
//        } catch (Exception e) {
//            LOGGER.error("从mysql加载extensionWord发⽣异常", e);
//        } finally {
//            if (resultSet != null) {
//                try {
//                    resultSet.close();
//                } catch (SQLException e) {
//                    LOGGER.error(e);
//                }
//            }
//            if (statement != null) {
//                try {
//                    statement.close();
//                } catch (SQLException e) {
//                    LOGGER.error(e);
//                }
//            }
//            if (connection != null) {
//                try {
//                    connection.close();
//                } catch (SQLException e) {
//                    LOGGER.error(e);
//                }
//            }
//        }
//    }
}


