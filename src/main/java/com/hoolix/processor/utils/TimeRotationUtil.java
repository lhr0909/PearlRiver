package com.hoolix.processor.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;
import java.util.Locale;

/**
 * Hoolix 2016
 * Created by simon on 11/14/16.
 */
public final class TimeRotationUtil {
    private static final Logger _log = Logger.getLogger(TimeRotationUtil.class);

    private static final String DEFAULT_PERIOD = "24hr";

    private static final PeriodFormatterBuilder PERIOD_FORMATTER_BUILDER = new PeriodFormatterBuilder()
            .appendMonths().appendSuffix("mo").appendSeparatorIfFieldsAfter(" ")
            .appendWeeks().appendSuffix("wk").appendSeparatorIfFieldsAfter(" ")
            .appendHours().appendSuffix("hr");

    private static final PeriodParser PERIOD_PARSER = PERIOD_FORMATTER_BUILDER.toParser();

    private static final String TIME_ROTATION_ZONE_STRING = "Asia/Shanghai";
    private static final DateTimeZone TIME_ROTATION_ZONE = DateTimeZone.forID(TIME_ROTATION_ZONE_STRING);
    private static final ISOChronology TIME_ROTATION_CHRONOLOGY = ISOChronology.getInstance(TIME_ROTATION_ZONE);

    private static final DateTimeFormatter TIME_ROTATION_FORMAT = DateTimeFormat.forPattern("yyMMddHH")
            .withLocale(Locale.ENGLISH)
            .withZone(TIME_ROTATION_ZONE)
            .withChronology(ISOChronology.getInstanceUTC());

    public static String getTimeRotationTimestamp(DateTime timestamp, String rotationPeriodString) {
        if (timestamp == null) {
            _log.error("there is no timestamp, returning _");
            return "_";
        }

        Period rotationPeriod = convertPeriodString(rotationPeriodString);

        return TIME_ROTATION_FORMAT.print(
                getTimestampFromRotationPeriod(
                        timestamp.withZone(TIME_ROTATION_ZONE),
                        roundPeriod(rotationPeriod)
                )
        );
    }

    public static boolean checkRotationPeriodString(String rotationPeriodString) {
        if (StringUtils.isBlank(rotationPeriodString)) {
            _log.warn("Period String is blank");
            return false;
        }

        Period rotationPeriod = convertPeriodString(rotationPeriodString);

        int months = rotationPeriod.getMonths();
        int weeks = rotationPeriod.getWeeks();
        int hours = rotationPeriod.getHours();

        //以最大的单位为基准,其余小单位会被舍弃
        if (months > 0) {
            //月份必须能整除12
            return (months <= 12) && (12 % months == 0);
        } else if (weeks > 0) {
            //周数只能是1和2
            return (weeks == 1) || (weeks == 2);
        } else {
            //小时数必须能整除24
            return (hours > 0) && (hours <= 24) && (24 % hours == 0);
        }
    }

    private static DateTime getTimestampFromRotationPeriod(DateTime timestamp, Period rotationPeriod) {
        int rotationMonths = rotationPeriod.getMonths();
        int rotationWeeks = rotationPeriod.getWeeks();
        int rotationHours = rotationPeriod.getHours();

        if (rotationMonths > 0) {
            return rotateByMonth(timestamp, rotationMonths);
        } else if (rotationWeeks > 0) {
            return rotateByWeek(timestamp, rotationWeeks);
        } else {
            return rotateByHour(timestamp, rotationHours);
        }
    }

    private static DateTime rotateByHour(DateTime timestamp, int rotationHours) {
        if (rotationHours >= 24) {
            //直接设成第二天的0点
            return new DateTime(
                    timestamp.getYear(), timestamp.getMonthOfYear(), timestamp.getDayOfMonth(),
                    0, 0, TIME_ROTATION_CHRONOLOGY
            ).plusDays(1);
        }

        int timestampHour = timestamp.getHourOfDay();

        int alignedHour = timestampHour / rotationHours * rotationHours;

        return new DateTime(
                timestamp.getYear(), timestamp.getMonthOfYear(), timestamp.getDayOfMonth(),
                alignedHour, 0, TIME_ROTATION_CHRONOLOGY
        ).plusHours(rotationHours);
    }

    private static DateTime rotateByWeek(DateTime timestamp, int rotationWeeks) {
        if (rotationWeeks >= 2) {
            //两周的逻辑是要么16号,要么下个月1号
            if (timestamp.getDayOfMonth() <= 15) {
                return new DateTime(
                        timestamp.getYear(), timestamp.getMonthOfYear(), 16,
                        0, 0, TIME_ROTATION_CHRONOLOGY
                );
            } else {
                return new DateTime(
                        timestamp.getYear(), timestamp.getMonthOfYear(), 1,
                        0, 0, TIME_ROTATION_CHRONOLOGY
                ).plusMonths(1);
            }
        }

        //找到时间戳所在周的下周一
        int alignedDays = DateTimeConstants.DAYS_PER_WEEK - timestamp.getDayOfWeek() + 1;

        return new DateTime(
                timestamp.getYear(), timestamp.getMonthOfYear(), timestamp.getDayOfMonth(),
                0, 0, TIME_ROTATION_CHRONOLOGY
        ).plusDays(alignedDays);
    }

    private static DateTime rotateByMonth(DateTime timestamp, int rotationMonths) {
        if (rotationMonths >= 12) {
            //直接设成第二年的1月1日
            return new DateTime(
                    timestamp.getYear() + 1,
                    1, 1, 0, 0, TIME_ROTATION_CHRONOLOGY
            );
        }

        int timestampMonth = timestamp.getMonthOfYear();

        int alignedMonth = ((timestampMonth - 1) / rotationMonths + 1) * rotationMonths;

        return new DateTime(
                timestamp.getYear(), alignedMonth, 1,
                0, 0, TIME_ROTATION_CHRONOLOGY
        ).plusMonths(1);
    }

    private static Period roundPeriod(Period period) {
        int months = period.getMonths();
        int weeks = period.getWeeks();
        int hours = period.getHours();

        ReadWritablePeriod result = new MutablePeriod();

        //以最大的单位为基准,其余小单位会被舍弃
        if (months > 12) {
            //超出12个月的返回12个月
            result.addMonths(12);
            return result.toPeriod();
        } else if (months > 0) {
            //找最近似的能整除的月
            result.addMonths(12 / (12 / months));
            return result.toPeriod();
        } else if (weeks > 0) {
            //找最近似的能整除的周
            result.addWeeks(4 / (4 / weeks));
            return result.toPeriod();
        } else if (hours > 0 && hours < 24) {
            //找最近似的能整除的小时
            result.addHours(24 / (24 / hours));
            return result.toPeriod();
        } else {
            //如果规则都不符合,返回24小时
            result.addHours(24);
            return result.toPeriod();
        }
    }

    private static Period convertPeriodString(String periodString) {
        if (StringUtils.isBlank(periodString)) {
            //如果输入有误,报错,并使用默认配置
            _log.warn("Period String is blank, using default 24hr");
            periodString = DEFAULT_PERIOD;
        }

        ReadWritablePeriod rotationPeriod = new MutablePeriod();
        PERIOD_PARSER.parseInto(rotationPeriod, periodString, 0, Locale.ENGLISH);
        return rotationPeriod.toPeriod();
    }

    public static void main(String[] args) {
        System.out.println(checkRotationPeriodString("6mo"));
        System.out.println(checkRotationPeriodString("2wk"));
        System.out.println(checkRotationPeriodString("12hr"));
        System.out.println();
    }
}
