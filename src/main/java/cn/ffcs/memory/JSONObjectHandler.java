package cn.ffcs.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class JSONObjectHandler implements ResultSetHandler<ObjectNode> {

    private SimpleDateFormat dateFormat;
    private ObjectMapper mapper = new ObjectMapper();

    public JSONObjectHandler() {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public void setDateFormat(SimpleDateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    /**
     * 结果集不包含数据时，返回空的JSON对象
     */
    @Override
    public ObjectNode handle(ResultSet rs) {
        try {
            ObjectNode object = mapper.createObjectNode();

            if (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsmd.getColumnLabel(i);
                    Object value = rs.getObject(columnName);
                    if (value == null)
                        object.putNull(columnName);
                    if (value instanceof Date) {
                        value = rs.getTimestamp(columnName);
                        object.put(columnName, dateFormat.format((Date) value));
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
            }

            return object;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
