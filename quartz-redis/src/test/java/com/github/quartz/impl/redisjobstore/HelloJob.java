package com.github.quartz.impl.redisjobstore;

import com.zaxxer.hikari.HikariDataSource;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ldang
 */
public class HelloJob implements Job {

    public static HikariDataSource dataSource = Util.getDataSource();

    private static final String INSERT_LOG = "insert into log(time, name) values (?, ?)";

    private static final String INSERT_INSTANCE = "insert into instance(time, name) values (?, ?)";

    private static final String yyyyMMddHHmmssSSS = "yyyy-MM-dd HH:mm:ss SSS";

    private static final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Date now = new Date();
        System.out.println(context.getJobDetail().getKey().getName() + "    "
                + new SimpleDateFormat(yyyyMMddHHmmssSSS).format(new Date()));
        String name = context.getJobDetail().getKey().getName();
        String time = new SimpleDateFormat(yyyyMMddHHmmss).format(now);
        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement ps1 = connection.prepareStatement(INSERT_LOG);
            ps1.setString(1, time);
            ps1.setString(2, name);
            ps1.execute();

            PreparedStatement ps2 = connection.prepareStatement(INSERT_INSTANCE);
            ps2.setString(1, time);
            ps2.setString(2, name);
            ps2.execute();

            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
