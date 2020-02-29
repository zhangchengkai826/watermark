package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class DataSet {
    private static final Logger LOGGER = LogManager.getLogger();

    private List<List<Object>> raw = new ArrayList<>();
    void addRow(List<Object> row) {
        raw.add(row);
    }
    List<Object> getRow(int index) {
        return raw.get(index);
    }
    int getNumRows() {
        return raw.size();
    }

    // Values in a fixed column will not be changed in the embedding process.
    // A fixed column's constraint is ignored.
    // Id is zero-based.
    private Set<Integer> fixedColId = new TreeSet<>();
    Set<Integer> getFixedColId() {
        return fixedColId;
    }

    static class ColumnDef {
        String name;

        enum Type {
            OTHER,
            INT4,
            FLOAT4,
            BPCHAR,
            DATE,
        }
        Type type = Type.OTHER;

        static class Constraint {
            // The embedded value E for original value O must satisfy O-magOfAlt <= S <= O+magOfAlt.
            // It only applies to integer or real type columns.
            // For integer type columns, it will be truncated.
            // It must be a positive value (for integer type columns, it must >= 1 due to truncation).
            float magOfAlt = 1.0f;

            // If it is true, the embbeder will try to make the embedded values' average as close to original values' average as possible.
            // It only applies to integer or real type columns.
            boolean tryKeepAvg = true;
        }
        Constraint constraint = new Constraint();
        
        ColumnDef(String name, String typeStr) {
            this.name = name;
            switch(typeStr) {
                case "int4": {
                    type = Type.INT4;
                    break;
                }
                case "float4": {
                    type = Type.FLOAT4;
                    break;
                }
                case "date": {
                    type = Type.DATE;
                    break;
                }
                case "bpchar": {
                    type = Type.BPCHAR;
                    break;
                }
                default: {
                    LOGGER.trace(name + ": " + typeStr);
                    type = Type.OTHER;
                }
            }
        }
    }
    private List<ColumnDef> colDef = new ArrayList<>();
    void addColDef(String name, String typeStr) {
        colDef.add(new ColumnDef(name, typeStr));
    }
    ColumnDef getColDef(int zeroBasedIndex) {
        return colDef.get(zeroBasedIndex);
    }

    void autoChooseFixedColumns() {
        if(colDef.isEmpty()) throw new RuntimeException("Cannot auto choose fixed columns when there is no column definitions.");
        for(int i = 0; i < colDef.size(); i++) {
            switch(colDef.get(i).type) {
                case BPCHAR:
                case DATE:
                case OTHER: {
                    fixedColId.add(i);
                    break;
                }
                default: {}
            }
        }
        if(fixedColId.isEmpty()) {
            fixedColId.add(0);
        }
    }
}
