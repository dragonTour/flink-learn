package util;

import joptsimple.internal.Strings;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class MapUtil {
    public static String toString(Object arg) {
        if (arg == null) {
            return "";
        } else {
            if (arg instanceof String) {
                return (String) arg;
            } else {
                return arg.toString();
            }
        }
    }

    public static int toInt(Object arg) {
        if (arg == null) {
            return 0;
        } else if (arg instanceof Integer) {
            return (Integer) arg;
        } else if (arg instanceof BigDecimal) {
            return ((BigDecimal) arg).intValue();
        } else if (arg instanceof Double) {
            return ((Double) arg).intValue();
        } else {
            return Integer.parseInt(arg.toString().trim().isEmpty() ? "0" : arg.toString().trim());
        }
    }

    public static long toLong(Object arg) {
        if (arg == null) {
            return 0;
        } else if (arg instanceof Long) {
            return (Long) arg;
        } else if (arg instanceof BigDecimal) {
            return ((BigDecimal) arg).longValue();
        } else if (arg instanceof Double) {
            return ((Double) arg).longValue();
        } else {
            return Long.parseLong(arg.toString().trim().isEmpty() ? "0" : arg.toString().trim());
        }
    }

    public static double toDouble(Object arg) {
        if (arg == null) {
            return 0;
        } else if (arg instanceof Double) {
            return (Double) arg;
        } else if (arg instanceof BigDecimal) {
            return ((BigDecimal) arg).doubleValue();
        } else {
            return Double.parseDouble(arg.toString().trim().isEmpty() ? "0" : arg.toString().trim());
        }
    }

    /**
     * 获取Map中子Map中的value
     *
     * @param ags  map
     * @param keys key
     * @return 返回String值
     */
    public static String deepGetString(Map<String, Object> ags, String... keys) {
        String targetKey = keys[keys.length - 1];
        Map<String, Object> map = deepGetMap(ags, (String[]) ArrayUtils.remove(keys, keys.length - 1));
        if (map != null) {
            return toString(map, targetKey);
        } else {
            return Strings.EMPTY;
        }
    }

    /**
     * 获取Map中子Map中的value
     *
     * @param ags  map
     * @param keys key
     * @return 返回Long值
     */
    public static Long deepGetLong(Map<String, Object> ags, String... keys) {
        String targetKey = keys[keys.length - 1];
        Map<String, Object> map = deepGetMap(ags, (String[]) ArrayUtils.remove(keys, keys.length - 1));
        if (map != null) {
            return toLong(map, targetKey);
        } else {
            return 0L;
        }
    }

    /**
     * 获取Map中子Map中的value
     *
     * @param ags  map
     * @param keys key
     * @return 返回Int值
     */
    public static Integer deepGetInt(Map<String, Object> ags, String... keys) {
        String targetKey = keys[keys.length - 1];
        Map<String, Object> map = deepGetMap(ags, (String[]) ArrayUtils.remove(keys, keys.length - 1));
        if (map != null) {
            return toInt(map, targetKey);
        } else {
            return 0;
        }
    }

    /**
     * 获取Map中子Map中的value
     *
     * @param ags  map
     * @param keys key
     * @return 返回Double值
     */
    public static Double deepGeDouble(Map<String, Object> ags, String... keys) {
        String targetKey = keys[keys.length - 1];
        Map<String, Object> map = deepGetMap(ags, (String[]) ArrayUtils.remove(keys, keys.length - 1));
        if (map != null) {
            return toDouble(map, targetKey);
        } else {
            return 0.00;
        }
    }

    /**
     * 获取Map中子Map
     *
     * @param ags  map
     * @param keys key
     * @return 返回map
     */
    private static Map deepGetMap(Map<String, Object> ags, String... keys) {
        //记录上层的map
        Map targetMap = ags;
        int i = 0;
        while (i < keys.length) {
            if (targetMap.get(keys[i]) == null || !(targetMap.get(keys[i]) instanceof Map)) {
                targetMap = null;
                break;
            } else {
                targetMap = (Map) targetMap.get(keys[i]);
            }
            i++;
        }
        return targetMap;
    }

    public static Object deepGetObject(Map<String, Object> map, String fieldsStr) {
        if (null == map) {
            return null;
        }
        String[] fields = fieldsStr.split("\\.");
        Map<String, Object> dataMap = map;
        int len = fields.length;
        for (int i = 0; i < len - 1; i++) {
            dataMap = (Map<String, Object>) dataMap.getOrDefault(fields[i], new HashMap<String, Object>());
            if (null == dataMap || dataMap.isEmpty()) {
                return null;
            }
        }
        return dataMap.get(fields[len - 1]);
    }

    public static String deepGetStringValue(Map<String, Object> map, String fieldsStr, String defVal) {
        Object object = deepGetObject(map,fieldsStr);
        if(null == object){
            return defVal;
        }
        return toString(object);
    }
    public static double deepGetDoubleValue(Map<String, Object> map, String fieldsStr, double defVal) {
        Object object = deepGetObject(map,fieldsStr);
        if(null == object){
            return defVal;
        }
        return toDouble(object);
    }

    public static int deepGetIntValue(Map<String, Object> map, String fieldsStr, int defVal) {
        Object object = deepGetObject(map,fieldsStr);
        if(null == object){
            return defVal;
        }
        return toInt(object);
    }

    public static long deepGetLongValue(Map<String, Object> map, String fieldsStr, long defVal) {
        Object object = deepGetObject(map,fieldsStr);
        if(null == object){
            return defVal;
        }
        return toLong(object);
    }

    public static Date toDate(Object arg) {
        return null;
    }

    public static long toLong(Map<String, Object> arg, String key) {
        return toLong(arg.get(key));
    }

    public static String toString(Map<String, Object> arg, String key) {
        return toString(arg.get(key));
    }

    public static int toInt(Map<String, Object> arg, String key) {
        return toInt(arg.get(key));
    }

    public static double toDouble(Map<String, Object> arg, String key) {
        return toDouble(arg.get(key));
    }

    public static void setMapValue(Map<String, Object> map, String fieldsStr, Object value) {

        String[] fields = fieldsStr.split("\\.");

        Map<String, Object> topMap = map;
        int len = fields.length;
        for (int i = 0; i < len; i++) {
            String key = fields[i];
            if (i == len - 1) {
                topMap.put(key, value);
            } else {
                Map<String, Object> dataMap = (Map<String, Object>) topMap.getOrDefault(key, new HashMap<String, Object>());
                if (dataMap.isEmpty()) {
                    topMap.put(key, dataMap);
                }
                topMap = dataMap;
            }
        }
    }
}
