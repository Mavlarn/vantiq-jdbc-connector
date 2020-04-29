package cn.ffcs.memory;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JSONArrayHandler implements ResultSetHandler<ArrayNode> {

    private boolean camel;
    private SimpleDateFormat sdf;

    private ObjectMapper mapper = new ObjectMapper();

    public JSONArrayHandler() {
        this(true);
    }

    public JSONArrayHandler(boolean camel) {
        this.camel = camel;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public ArrayNode handle(ResultSet rs) {
        try {
            ArrayNode array = mapper.createArrayNode();

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                if (columnCount == 1) {
                    array.add(rs.getObject(1).toString());
                    continue;
                }
                ObjectNode object = mapper.createObjectNode();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsmd.getColumnLabel(i);
                    if (camel) {
                        columnName = underscore2Camel(columnName);
                    }
                    Object value = rs.getObject(columnName);
                    if (value == null) {
                        object.putNull(columnName);
                    } else if (value instanceof Date) {
                        // object.put(columnName, ((Date) value).getTime());
                        value = rs.getTimestamp(columnName);
                        object.put(columnName, sdf.format((Date) value));
                    } else if (value instanceof Clob) {
                        Clob clob = (Clob) value;
                        object.put(columnName, clob.getSubString((long) 1, (int) clob.length()));
                    } else if (value instanceof Integer) {
                        object.put(columnName, (Integer) value);
                    } else if (value instanceof String) {
                        object.put(columnName, (String) value);
                    } else if (value instanceof Boolean) {
                        object.put(columnName, (Boolean) value);
                    } else if (value instanceof Long) {
                        object.put(columnName, (Long) value);
                    } else if (value instanceof Double) {
                        object.put(columnName, (Double) value);
                    } else if (value instanceof Float) {
                        object.put(columnName, (Float) value);
                    } else if (value instanceof BigDecimal) {
                        object.put(columnName, (BigDecimal) value);
                    } else if (value instanceof Byte) {
                        object.put(columnName, (Byte) value);
                    } else if (value instanceof byte[]) {
                        object.put(columnName, (byte[]) value);
                    } else {
                        throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
                    }


                }
                array.add(object);
            }
            return array;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String underscore2Camel(String underscore) {
        StringBuffer buf = new StringBuffer();
        underscore = underscore.toLowerCase();
        Matcher m = Pattern.compile("_([a-z])").matcher(underscore);
        while (m.find()) {
            m.appendReplacement(buf, m.group(1).toUpperCase());
        }
        return m.appendTail(buf).toString();
    }

}
