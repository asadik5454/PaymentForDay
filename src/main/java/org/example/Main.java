package org.example;

import java.sql.*;
import java.time.LocalDateTime;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {


        String SQL = "select \n" +
                "unique(s.range_map_external_id) as subs_key,\n" +
                "v.display_value as service,\n" +
                "(r.rate/10000) as price,\n" +
                "TO_CHAR(sysdate, 'DD.MM.YYYY') as cur_date,\n" +
                "TO_CHAR(t.next_apply_dt, 'DD.MM.YYYY')as next_apply_day\n" +
                " from account_subscriber s   JOIN\n" +
                "RC_TERM_INST t on s.subscr_no = t.parent_subscr_no  JOIN\n" +
                "rate_rc r on t.rc_term_id = r.rc_term_id  JOIN\n" +
                "RC_TERM_VALUES v on t.rc_term_id = v.rc_term_id\n" +
                "where t.status = '2'\n" +
                "and t.processing_status = '1'\n" +
                "and r.reseller_version_id =(select max(reseller_version_id) from rate_rc)\n" +
                "and (r.rate/10000) > '0'\n" +
                "and r.currency_code = '131'\n" +
                "and t.period_frequency >= 2\n" +
                "and trunc(t.next_apply_dt) = trunc(sysdate+1)\n" +
                "and t.next_apply_dt is not null";

        Class.forName("oracle.jdbc.OracleDriver");
        Connection conMain1 = DriverManager.getConnection("jdbc:oracle:thin:@172.28.248.4:1521:main", "CBS_OWNER", "Comverse");
        Statement statement = conMain1.createStatement();
        Connection con2 = DriverManager.getConnection("jdbc:postgresql://tjk-zetalif01:5432/zetalifDB",
                "zetalif", "JnmnF54D3Op");
        System.out.println("ResultSet start NOW :  "  + SQL);
        ResultSet resultset = statement.executeQuery(SQL);
        int id = 0;

        String Name = "";
        String Number = "";
        String Amount = "";
        String nextApplyDay = "";

        while (resultset.next()) {
            id++;
            Number = resultset.getString("SUBS_KEY");
            Name = resultset.getString("SERVICE");
            Amount = resultset.getString("PRICE");
            nextApplyDay = resultset.getString("NEXT_APPLY_DAY");
            System.out.println(id + "     " + Number.substring(3) + "  " + Name + "   " + Amount +  "   "   + nextApplyDay);
            Class.forName("org.postgresql.Driver");

            String SQL2 = "INSERT INTO public.payment_per_day(number, tech_name, amount,next_apply_date)\r\n" +
                    "VALUES('" + Number + "', '" + Name + "','" + Amount + "','" + nextApplyDay + "');";

            try {
                PreparedStatement ps = con2.prepareStatement(SQL2);
                ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        Statement statementSelect = con2.createStatement();
        boolean resultSet = statementSelect.execute("update payment_per_day a\n" +
                "set commercial_name=(select b.commercial_name from services_settings b where b.periodical_name=a.tech_name) where commercial_name is null;");
        statement.close();
        conMain1.close();

        System.out.println("2");
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conMain3 = DriverManager.getConnection("jdbc:oracle:thin:@172.28.248.16:1521:main", "CBS_OWNER", "Comverse");
        Statement statementMain3 = conMain3.createStatement();

        ResultSet resultsetMain3 = statementMain3.executeQuery(SQL);
        while (resultsetMain3.next()) {
            id++;
            Number = resultsetMain3.getString("SUBS_KEY");
            Name = resultsetMain3.getString("SERVICE");
            Amount = resultsetMain3.getString("PRICE");
            nextApplyDay = resultsetMain3.getString("NEXT_APPLY_DAY");
            System.out.println(id + "     " + Number.substring(3) + "  " + Name + "   " + Amount);
            Class.forName("org.postgresql.Driver");
            String SQL2 = "INSERT INTO public.payment_per_day(number, tech_name, amount,next_apply_date)\r\n" +
                    "VALUES('" + Number + "', '" + Name + "','" + Amount + "','" + nextApplyDay + "');";

            try {
                PreparedStatement ps = con2.prepareStatement(SQL2);

                ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }
        Statement statementSelect1 = con2.createStatement();
        boolean resultSet1 = statementSelect1.execute("update payment_per_day a\n" +
                "set commercial_name=(select b.commercial_name from services_settings b where b.periodical_name=a.tech_name) where commercial_name is null;");

        statementSelect.close();
        con2.close();
        statement.close();
        conMain3.close();

    }



    }
