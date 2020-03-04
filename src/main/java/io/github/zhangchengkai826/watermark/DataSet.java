package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class DataSet {
    private static final Logger LOGGER = LogManager.getLogger();

    private List<List<Object>> raw = new ArrayList<>();

    public void addRow(List<Object> row) {
        raw.add(row);
    }

    public List<Object> getRow(int index) {
        return raw.get(index);
    }

    public int getNumRows() {
        return raw.size();
    }

    // Values in a fixed column will not be changed in the embedding process.
    // A fixed column's constraint is ignored.
    // Id is zero-based.
    private Set<Integer> fixedColId = new TreeSet<>();

    public Set<Integer> getFixedColId() {
        return fixedColId;
    }

    // It throws exceptions if no column with colName is found.
    public void setColumnAsFixed(String colName) {
        fixedColId.add(getColIdByName(colName));
    }

    public void setColumnAsFixed(String[] colNames) {
        for (String name : colNames)
            fixedColId.add(getColIdByName(name));
    }

    public boolean isColumnFixed(int colId) {
        return fixedColId.contains(colId);
    }

    public void setColumnConstraintMagOfAlt(String colName, float value) {
        int colId = getColIdByName(colName);
        getColDef(colId).setConstraintMagOfAlt(value);
    }

    static class ColumnDef {
        String name;

        enum Type {
            OTHER, INT4, FLOAT4, BPCHAR, DATE,
        }

        Type type = Type.OTHER;

        static class Constraint {
            // The embedded value E for original value O must satisfy O-magOfAlt <= S <=
            // O+magOfAlt.
            //
            // It must be a positive value.
            //
            // It only applies to integer or real type columns.
            //
            // For now, integer type columns are converted to real type columns when
            // embedding, and truncated back to integer type after embedding, so if you want
            // to reduce the possibility of watermark corruption caused by truncation,
            // please set this value high (> 10) when possible for integer type columns.
            private float magOfAlt = 1.0f;

            float getMagOfAlt() {
                return magOfAlt;
            }

            void setMagOfAlt(float value) {
                magOfAlt = value;
            }

            // If it is true, the embbeder will try to make the embedded values' average as
            // close to original values' average as possible.
            //
            // It only applies to integer or real type columns.
            private boolean tryKeepAvg = false;

            boolean getTryKeepAvg() {
                return tryKeepAvg;
            }
        }

        private Constraint constraint = new Constraint();

        Constraint getConstraint() {
            return constraint;
        }

        void setConstraintMagOfAlt(float value) {
            constraint.magOfAlt = value;
        }

        ColumnDef(String name, String typeStr) {
            this.name = name;
            switch (typeStr) {
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

    public int getNumCols() {
        return colDef.size();
    }

    ColumnDef getColDef(int zeroBasedIndex) {
        return colDef.get(zeroBasedIndex);
    }

    // It returns zero-based id, and throws exception if no such id is found.
    int getColIdByName(String name) {
        int id = findColIdByName(name);
        if (id < 0)
            throw new NoSuchElementException("No column with such name is found.");
        return id;
    }

    // It returns zero-based id, and returns negative value if no such id is found.
    public int findColIdByName(String name) {
        return IntStream.range(0, getNumCols()).filter(i -> getColDef(i).name.equals(name)).findFirst().orElse(-1);
    }

    void autoChooseFixedColumns() {
        if (colDef.isEmpty())
            throw new RuntimeException("Cannot auto choose fixed columns when there is no column definitions.");
        for (int i = 0; i < colDef.size(); i++) {
            switch (colDef.get(i).type) {
                case BPCHAR:
                case DATE:
                case OTHER: {
                    fixedColId.add(i);
                    break;
                }
                default: {
                }
            }
        }
        if (fixedColId.isEmpty()) {
            fixedColId.add(0);
        }
    }

    DataSet getLinkedCopyWithIndependentRaw() {
        DataSet copy = new DataSet();
        copy.colDef = colDef;
        copy.fixedColId = fixedColId;
        return copy;
    }

    List<Double> getColumnAsDataVec(String colName) {
        return getColumnAsDataVec(getColIdByName(colName));
    }

    List<Double> getColumnAsDataVec(int colId) {
        ColumnDef colDef = getColDef(colId);
        List<Double> dataVec = new ArrayList<>();
        for (int j = 0; j < getNumRows(); j++) {
            switch (colDef.type) {
                case INT4: {
                    dataVec.add(((Integer) getRow(j).get(colId)).doubleValue());
                    break;
                }
                case FLOAT4: {
                    dataVec.add(((Float) getRow(j).get(colId)).doubleValue());
                    break;
                }
                default: {
                    LOGGER.trace("Column Name: " + colDef.name + ", Column Type: " + colDef.type);
                    throw new RuntimeException(
                            "For now, only integer or real type columns can be converted into dataVec.");
                }
            }
        }
        return dataVec;
    }

    void setColumnByDataVec(String colName, List<Double> dataVec) {
        setColumnByDataVec(getColIdByName(colName), dataVec);
    }

    void setColumnByDataVec(int colId, List<Double> dataVec) {
        ColumnDef colDef = getColDef(colId);
        for (int j = 0; j < getNumRows(); j++) {
            Object dataElem;
            switch (colDef.type) {
                case INT4: {
                    dataElem = dataVec.get(j).intValue();
                    break;
                }
                case FLOAT4: {
                    dataElem = dataVec.get(j).floatValue();
                    break;
                }
                default: {
                    throw new RuntimeException(
                            "For now, dataVec can only be coverted into integer or real type columns.");
                }
            }
            getRow(j).set(colId, dataElem);
        }
    }
}
